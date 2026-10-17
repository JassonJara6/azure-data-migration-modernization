"""Offline checks only. No SQL Server, Azure, network, or credentials.

Predicate tests interpret the actual planner's concatenation fragments and execute
those comparisons against synthetic SQLite rows; they do not compile T-SQL.
"""
import copy
import importlib.util
import itertools
import json
import random
import re
import sqlite3
import unittest
from pathlib import Path

from adf_expression import evaluate, condition_status

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('build_adf', ROOT / 'adf/build_artifacts.py')
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)
ARTIFACTS = BUILD.artifacts()
SQL = (ROOT / 'adf/sql/plan_bronze_batch.sql').read_text()
PARENT = ARTIFACTS['pipeline/pl_adventureworks_bronze.json']['properties']
CHILD = ARTIFACTS['pipeline/pl_adventureworks_bronze_table.json']['properties']


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def activity(properties, name):
    return next(a for a in properties['activities'] if a['name'] == name)


def expression_tokens(text):
    """Check quote/parenthesis balance, preserving ADF doubled single quotes."""
    tokens = re.findall(r"'(?:''|[^'])*'|[A-Za-z_][A-Za-z_0-9]*|\d+|[^\s]", text)
    depth = 0
    for token in tokens:
        if token == "'":
            raise AssertionError('Unterminated ADF string')
        if token == '(':
            depth += 1
        elif token == ')':
            depth -= 1
            assert depth >= 0
    assert depth == 0
    return tokens


def concat_expression(text, variables):
    """Small interpreter for only the SQL planner's predicate-building subset."""
    parts = re.split(r"\+(?=(?:[^']*'(?:[^']|'')*')*[^']*$)", text.strip())
    out = []
    for part in parts:
        part = part.strip()
        if re.fullmatch(r"N?'(?:''|[^'])*'", part):
            start = 2 if part.startswith('N') else 1
            out.append(part[start:-1].replace("''", "'"))
        elif re.fullmatch(r'@[a-z0-9_]+', part):
            out.append(str(variables[part]))
        elif re.fullmatch(r'QUOTENAME\(@[a-z0-9_]+\)', part):
            out.append('[' + variables[part[10:-1]] + ']')
        else:
            match = re.fullmatch(r'CONVERT\(nvarchar\(20\),(@[a-z0-9_]+)\)', part)
            if not match:
                raise AssertionError('Unsupported predicate expression: ' + part)
            out.append(str(variables[match[1]]))
    return ''.join(out)


def rendered_predicate(lower, upper, composite, sql=SQL, names=('PK1', 'PK2')):
    variables = {'@k1': names[0], '@k2': names[1], '@l1': lower[1], '@u1': upper[1],
                 '@l2': lower[2] if composite else 0, '@u2': upper[2] if composite else 0,
                 '@wm': '[ModifiedDate]', '@ld': (repr(lower[0]) if isinstance(lower[0], str) else str(lower[0])),
                 '@ud': (repr(upper[0]) if isinstance(upper[0], str) else str(upper[0]))}
    declaration = re.search(r'DECLARE @lk nvarchar\(600\)=(.*?);', sql, re.S)[1]
    variables['@lk'] = concat_expression(declaration, variables)
    if composite:
        compound = re.search(r'IF @k2 IS NOT NULL SET @lk=(.*?);', sql, re.S)[1]
        variables['@lk'] = concat_expression(compound, variables)
    upper_block = re.search(r'IF @k2 IS NULL\s+SET @uk=(.*?);\s+ELSE\s+SET @uk=(.*?);', sql, re.S)
    variables['@uk'] = concat_expression(upper_block[2 if composite else 1], variables)
    lower_expr = re.search(r"SET @where=(N'\('\+@wm.*?);", sql, re.S)[1]
    variables['@where'] = concat_expression(lower_expr, variables)
    upper_expr = re.search(r'SET @where=(@where\+.*?);', sql, re.S)[1]
    return concat_expression(upper_expr, variables)


class ArtifactTests(unittest.TestCase):
    def test_generated_files_match_sources(self):
        for path, value in ARTIFACTS.items():
            self.assertEqual((ROOT / 'adf' / path).read_text(), json.dumps(value, indent=2) + '\n')

    def test_references_and_parameter_bindings(self):
        datasets = {v['name']: v['properties'] for k, v in ARTIFACTS.items() if k.startswith('dataset/')}
        pipelines = {v['name']: v['properties'] for k, v in ARTIFACTS.items() if k.startswith('pipeline/')}
        for artifact in ARTIFACTS.values():
            for node in walk(artifact):
                kind = node.get('type')
                if kind == 'DatasetReference':
                    self.assertEqual(set(node['parameters']), set(datasets[node['referenceName']]['parameters']))
                if kind == 'LinkedServiceReference':
                    self.assertIn(node['referenceName'], ['ls_sql_server', 'ls_adls_gen2'])
                if kind == 'ExecutePipeline':
                    properties = node['typeProperties']
                    self.assertTrue(properties['waitOnCompletion'])
                    self.assertEqual(set(properties['parameters']),
                                     set(pipelines[properties['pipeline']['referenceName']]['parameters']))
                if kind == 'Expression':
                    self.assertTrue(node['value'].startswith('@'))
                    expression_tokens(node['value'])

    def test_dependency_order_and_isolation(self):
        for properties in [PARENT, CHILD]:
            seen = set()
            for node in properties['activities']:
                for dependency in node.get('dependsOn', []):
                    self.assertIn(dependency['activity'], seen)
                    self.assertEqual(dependency['dependencyConditions'], ['Succeeded'])
                seen.add(node['name'])
            self.assertEqual(properties['concurrency'], 1)
        self.assertNotIn('variables', PARENT)
        self.assertTrue(all(v['type'] == 'String' for v in CHILD['variables'].values()))
        self.assertFalse(any(n.get('type') in ['SetVariable', 'AppendVariable'] for n in walk(ARTIFACTS)))
        self.assertTrue(activity(PARENT, 'ForEachSource')['typeProperties']['isSequential'])
        lookup = activity(PARENT, 'ReadActiveConfigurations')['typeProperties']
        self.assertFalse(lookup['firstRowOnly'])
        self.assertIn('WHERE is_active=1', lookup['source']['sqlReaderQuery'])

    def test_no_cursor_writes_and_fail_closed(self):
        for properties in [PARENT, CHILD]:
            self.assertFalse(properties['parameters']['acknowledge_landing_only']['defaultValue'])
            gate = activity(properties, 'AcknowledgeLandingOnly')['typeProperties']
            self.assertEqual(gate['ifFalseActivities'][0]['type'], 'Fail')
        block = PARENT['activities'][-1]
        self.assertEqual(block['typeProperties']['errorCode'], 'WATERMARK_CONFIRMATION_BLOCKED')
        self.assertEqual(block['dependsOn'][0]['activity'], 'ForEachSource')
        types = {n.get('type') for n in walk(ARTIFACTS)}
        self.assertFalse(types & {'Script', 'WebActivity', 'Delete'})
        code = re.sub(r"--[^\n]*|'(?:''|[^'])*'", '', SQL)
        self.assertIsNone(re.search(r'\b(UPDATE|DELETE|MERGE|TRUNCATE|DROP|GRANT|CREATE|ALTER)\b', code, re.I))
        self.assertNotRegex(code, r'INSERT\s+ingestion\.')
        self.assertNotRegex(SQL, r'(?i)\bUSE\s+\[')
        self.assertIn("IF DB_NAME()<>N'AdventureWorks'", SQL)

    def test_sql_template_lexical_sanity(self):
        code = re.sub(r"--[^\n]*|'(?:''|[^'])*'", '', SQL)
        self.assertNotIn("'", code)
        depth = 0
        for char in code:
            depth += (char == '(') - (char == ')')
            self.assertGreaterEqual(depth, 0)
        self.assertEqual(depth, 0)
        self.assertEqual(SQL.count('__CONFIG_ID__'), 1)
        self.assertNotIn('GO\n', code)

    def test_embedded_sql_exact_round_trip(self):
        plan = activity(CHILD, 'PlanBatch')['typeProperties']['source']['sqlReaderQuery']['value']
        context = {'pipeline': {'parameters': {'config_id': 123}},
                   'variables': {k: v['defaultValue'] for k, v in CHILD['variables'].items()}}
        compiled = evaluate(plan, context)
        self.assertEqual(compiled, SQL.replace('__CONFIG_ID__', '123'))
        self.assertEqual(CHILD['parameters']['config_id']['type'], 'Int')

    def test_seed_scope_and_real_key_validation(self):
        seed = (ROOT / 'sql/metadata/002_seed_ingestion_metadata.sql').read_text()
        seed_rows = re.findall(r"\(N'([^']+)', N'([^']+)', N'(FULL|INCREMENTAL)', N'([^']+)'", seed)
        plan_rows = re.findall(r"\(N'([^']+)',N'([^']+)','(FULL|INCREMENTAL)',N'([^']+)'\)", SQL)
        self.assertEqual(set(seed_rows), set(plan_rows))
        self.assertEqual(len(plan_rows), 11)
        self.assertEqual(sum(r[2] == 'INCREMENTAL' for r in plan_rows), 5)
        for text in ['sys.index_columns', 'key_ordinal', 'QUOTENAME', 'TOP (1)',
                     'ORDER BY [ModifiedDate] DESC,', '@upper=@lower, @u1=@l1, @u2=@l2',
                     'expected_row_version', 'last_successful_at_utc', 'Latin1_General_100_BIN2']:
            self.assertIn(text, SQL)
        self.assertNotIn('MAX(', SQL)

    def test_destination_and_copy_guards(self):
        sink = ARTIFACTS['dataset/ds_bronze_parquet.json']['properties']
        self.assertEqual(sink['type'], 'Parquet')
        self.assertEqual(sink['typeProperties']['location']['fileSystem'], 'bronze')
        self.assertEqual(sink['typeProperties']['location']['fileName'], 'part-00000.parquet')
        copy = activity(CHILD, 'CopyToBronze')
        self.assertEqual(copy['policy']['retry'], 0)
        self.assertEqual(copy['typeProperties']['parallelCopies'], 1)
        self.assertFalse(copy['typeProperties']['enableSkipIncompatibleRow'])
        self.assertFalse(copy['typeProperties']['translator']['typeConversionSettings']['allowDataTruncation'])
        folder = copy['outputs'][0]['parameters']['folder_path']['value']
        for text in ['bronze_relative_path', '/load_type=', '/ingestion_date=', '/run_id=', 'pipeline().RunId']:
            self.assertIn(text, folder)
        check = activity(CHILD, 'CheckCopyCounters')['typeProperties']
        for text in ['rowsCopied', 'rowsRead', 'expected_rows']:
            self.assertIn(text, check['expression']['value'])
        self.assertEqual(check['ifFalseActivities'][0]['type'], 'Fail')
        self.assertIn('c.system_type_id=241', SQL)
        self.assertIn("t.name=N'hierarchyid'", SQL)
        self.assertIn("N'.Serialize()'", SQL)


def sample_context():
    row = dict(zip(BUILD.PLAN_FIELDS, [1, 123, 'Sales', 'SalesOrderDetail', 'INCREMENTAL',
        'adventureworks/sales/salesorderdetail', '8', '2026-10-14T00:00:00.0000001',
        '{"SalesOrderID":10,"SalesOrderDetailID":1}', '2026-10-14T00:00:00.0000002',
        '{"SalesOrderID":20,"SalesOrderDetailID":5}', '0x0000000000000001', 'SELECT 1;']))
    return {'pipeline': {'RunId': 'child-run-id', 'TriggerTime': '2026-10-14T23:50:00Z',
                         'parameters': {'config_id': 123, 'acknowledge_landing_only': True,
                                        'parent_run_id': 'parent-run-id', 'ingestion_date': '2026-10-14'}},
            'variables': {k: v['defaultValue'] for k, v in CHILD['variables'].items()},
            'activities': {'PlanBatch': {'output': {'firstRow': row}},
                           'ReadActiveConfigurations': {'output': {'count': '1', 'value': [{'config_id': '123'}]}},
                           'CopyToBronze': {'output': {'rowsRead': 8, 'rowsCopied': 8}}},
            'item': {'config_id': '123'}, 'dataset': {'folder_path': 'a/b', 'source_schema': 'Sales',
                                                    'source_table': 'SalesOrderDetail'},
            'utcNow': '2026-10-14T23:51:00Z'}


class LookupAndControlTests(unittest.TestCase):
    def test_all_actual_expressions_evaluate_with_typed_contracts(self):
        context = sample_context()
        for artifact in ARTIFACTS.values():
            properties = artifact['properties']
            for node in walk(artifact):
                if node.get('type') == 'Expression':
                    result = evaluate(node, context)
                    if not node['value'].startswith('@if('):
                        self.assertIsNotNone(result)
                    for parameter in re.findall(r'pipeline\(\).parameters.([A-Za-z_]+)', node['value']):
                        self.assertIn(parameter, properties['parameters'])
        execute = activity(PARENT, 'ForEachSource')['typeProperties']['activities'][0]['typeProperties']
        forwarded = {k: evaluate(v, context) for k, v in execute['parameters'].items()}
        for name, value in forwarded.items():
            self.assertIs(type(value), {'Int': int, 'Bool': bool, 'String': str}[CHILD['parameters'][name]['type']])
        self.assertEqual(forwarded['config_id'], 123)
        context['item']['config_id'] = '123; DROP TABLE x'
        with self.assertRaises(ValueError):
            evaluate(execute['parameters']['config_id'], context)

    def test_size_budgets_and_expression_regression(self):
        BUILD.validate_artifacts(ARTIFACTS)
        longest = max(len(n['value']) for n in walk(ARTIFACTS) if n.get('type') == 'Expression')
        self.assertLessEqual(longest, 8192)
        for variable in CHILD['variables'].values():
            self.assertLessEqual(len(variable['defaultValue']), BUILD.SQL_CHUNK_CHARS)
        broken = copy.deepcopy(ARTIFACTS)
        child = broken['pipeline/pl_adventureworks_bronze_table.json']['properties']
        activity(child, 'PlanBatch')['typeProperties']['source']['sqlReaderQuery'] = BUILD.expr('@concat(\'' + 'x' * 8192 + '\')')
        with self.assertRaisesRegex(ValueError, '8192'):
            BUILD.validate_artifacts(broken)

    def test_lookup_schema_and_worst_case_escaped_payload(self):
        # 32768 UTF-16 units at six escaped bytes each plus bounded metadata.
        self.assertIn('DATALENGTH(@extract_query)>65536', SQL)
        worst = sample_context()['activities']['PlanBatch']['output']['firstRow']
        for key in worst:
            if isinstance(worst[key], str):
                worst[key] = '\x01' * (32768 if key == 'extract_query' else 1000)
        self.assertLess(len(json.dumps({'firstRow': worst}).encode()), 4 * 1024 * 1024)
        self.assertTrue(all(not isinstance(value, bytes) for value in worst.values()))
        projection = SQL[SQL.index('SELECT 1 AS plan_contract_version,'):]
        self.assertEqual(set(re.findall(r' AS ([a-z_]+)', projection)), set(BUILD.PLAN_FIELDS))
        query = activity(PARENT, 'ReadActiveConfigurations')['typeProperties']['source']['sqlReaderQuery']
        self.assertIn('SELECT TOP (12) config_id', query)
        self.assertIn(')>11', query)
        self.assertFalse(activity(PARENT, 'ReadActiveConfigurations')['typeProperties']['firstRowOnly'])
        self.assertTrue(activity(CHILD, 'PlanBatch')['typeProperties']['firstRowOnly'])
        self.assertIn("CONVERT(varchar(18),CONVERT(binary(8),s.row_version),1)", SQL)

    def test_database_guard_and_copy_single_select(self):
        parent_query = activity(PARENT, 'ReadActiveConfigurations')['typeProperties']['source']['sqlReaderQuery']
        for query in [SQL, parent_query]:
            self.assertNotRegex(query, r'(?i)\bUSE\s+\[')
            self.assertIn("DB_NAME()<>N'AdventureWorks'", query)
            self.assertIn('SET NOCOUNT ON;', query)
        extract = re.search(r'DECLARE @extract_query nvarchar\(max\)=(.*?);\nIF @extract_query', SQL, re.S)[1]
        query = concat_expression(extract, {'@columns': '[SalesOrderID],[SalesOrderDetailID]',
                                           '@qualified': '[Sales].[SalesOrderDetail]', '@where': '1=1',
                                           '@order': '[SalesOrderID],[SalesOrderDetailID]'})
        self.assertEqual(query, 'SELECT [SalesOrderID],[SalesOrderDetailID] FROM [Sales].[SalesOrderDetail] WHERE 1=1 ORDER BY [SalesOrderID],[SalesOrderDetailID];')
        self.assertEqual(query.count(';'), 1)
        self.assertNotIn('USE ', query)

    def test_acknowledgement_and_count_failures_propagate(self):
        context = sample_context()
        for properties in [PARENT, CHILD]:
            gate = activity(properties, 'AcknowledgeLandingOnly')
            self.assertEqual(condition_status(gate, context), 'Succeeded')
            context['pipeline']['parameters']['acknowledge_landing_only'] = False
            self.assertEqual(condition_status(gate, context), 'Failed')
            context['pipeline']['parameters']['acknowledge_landing_only'] = 'false'
            with self.assertRaises(TypeError):
                condition_status(gate, context)
            context['pipeline']['parameters']['acknowledge_landing_only'] = True
        check = activity(CHILD, 'CheckCopyCounters')
        self.assertEqual(condition_status(check, context), 'Succeeded')
        for read, copied in [(7, 8), (8, 7), (9, 9)]:
            context['activities']['CopyToBronze']['output'] = {'rowsRead': read, 'rowsCopied': copied}
            self.assertEqual(condition_status(check, context), 'Failed')
        context['activities']['PlanBatch']['output']['firstRow']['expected_rows'] = '0'
        context['activities']['CopyToBronze']['output'] = {'rowsRead': 0, 'rowsCopied': 0}
        self.assertEqual(condition_status(check, context), 'Succeeded')
        context['activities']['CopyToBronze']['output'] = {}
        with self.assertRaises(KeyError):
            condition_status(check, context)

    def test_active_lookup_rejects_empty_oversized_or_truncated_arrays(self):
        context = sample_context()
        gate = activity(PARENT, 'ValidateActiveConfigurations')
        for size, expected in [(0, 'Failed'), (1, 'Succeeded'), (11, 'Succeeded'), (12, 'Failed')]:
            output = {'count': str(size), 'value': [{'config_id': n} for n in range(size)]}
            context['activities']['ReadActiveConfigurations']['output'] = output
            self.assertEqual(condition_status(gate, context), expected)
        output['count'], output['value'] = '12', [{'config_id': 1}]
        self.assertEqual(condition_status(gate, context), 'Failed')
        self.assertEqual(activity(PARENT, 'ForEachSource')['dependsOn'], BUILD.after('ValidateActiveConfigurations'))

    def test_registration_is_bound_and_full_cursor_parameters_are_null(self):
        register = activity(CHILD, 'RegisterCopyAudit')
        self.assertEqual(register['type'], 'SqlServerStoredProcedure')
        self.assertEqual(register['dependsOn'], BUILD.after('CheckCopyCounters'))
        self.assertEqual(register['typeProperties']['storedProcedureName'], 'ingestion.register_bronze_copy')
        self.assertEqual(register['policy']['retry'], 2)
        self.assertTrue(register['policy']['secureInput'])
        self.assertNotIn('scripts', register['typeProperties'])
        parameters = register['typeProperties']['storedProcedureParameters']
        self.assertEqual(set(parameters), {'config_id','parent_run_id','child_run_id','bronze_path','load_type',
            'expected_rows','copied_rows','watermark_lower','watermark_lower_key_json','watermark_upper',
            'watermark_upper_key_json','expected_row_version'})
        context = sample_context()
        self.assertIs(type(evaluate(parameters['config_id']['value'], context)), int)
        for mode in ['INCREMENTAL', 'FULL']:
            context['activities']['PlanBatch']['output']['firstRow']['load_type'] = mode
            for name in ['watermark_lower','watermark_upper','watermark_lower_key_json','watermark_upper_key_json','expected_row_version']:
                value = evaluate(parameters[name]['value'], context)
                self.assertEqual(value is None, mode == 'FULL')
        procs = [node['typeProperties']['storedProcedureName'] for node in walk(ARTIFACTS) if node.get('type') == 'SqlServerStoredProcedure']
        self.assertEqual(procs, ['ingestion.register_bronze_copy'])

    def test_plan_guard_blocks_wrong_version_id_mode_count_or_query(self):
        context = sample_context()
        gate = activity(CHILD, 'ValidatePlan')
        row = context['activities']['PlanBatch']['output']['firstRow']
        self.assertEqual(condition_status(gate, context), 'Succeeded')
        row['plan_contract_version'], row['config_id'] = '1', '123'
        self.assertEqual(condition_status(gate, context), 'Succeeded')
        for name, bad in [('plan_contract_version', 2), ('config_id', 124), ('load_type', 'UNKNOWN'),
                          ('expected_rows', '-1'), ('extract_query', ''), ('extract_query', 'x' * 32769)]:
            old, row[name] = row[name], bad
            self.assertEqual(condition_status(gate, context), 'Failed', name)
            row[name] = old
        del row['lower_key_json']
        self.assertEqual(condition_status(gate, context), 'Failed')
        del row['extract_query']
        with self.assertRaises(KeyError):
            condition_status(gate, context)
        self.assertEqual(activity(CHILD, 'CopyToBronze')['dependsOn'], BUILD.after('ValidatePlan'))


class CursorPredicateTests(unittest.TestCase):
    def detail_window(self, sql=SQL):
        stamp = '2026-10-14T12:00:00.1234567'
        lower, upper = (stamp, 100, 5), (stamp, 102, 10)
        rows = [('2026-10-14T12:00:00.1234566', 999, 999),
                (stamp, 99, 999), (stamp, 100, 4), (stamp, 100, 5),
                (stamp, 100, 6), (stamp, 101, 999), (stamp, 102, 1),
                (stamp, 102, 10), (stamp, 102, 11), (stamp, 103, 1),
                (stamp, 103, 999), ('2026-10-14T12:00:00.1234568', 1, 1)]
        predicate = rendered_predicate(lower, upper, True, sql, ('SalesOrderID', 'SalesOrderDetailID'))
        with sqlite3.connect(':memory:') as db:
            db.execute("ATTACH DATABASE ':memory:' AS Sales")
            db.execute('CREATE TABLE Sales.SalesOrderDetail (ModifiedDate TEXT, SalesOrderID INT, SalesOrderDetailID INT)')
            db.executemany('INSERT INTO Sales.SalesOrderDetail VALUES (?,?,?)', rows)
            # Interpret the actual final Copy query concatenation, rather than
            # testing a separately maintained Python comparison implementation.
            extract = re.search(r'DECLARE @extract_query nvarchar\(max\)=(.*?);\nIF @extract_query', sql, re.S)[1]
            query = concat_expression(extract, {'@columns': '[ModifiedDate],[SalesOrderID],[SalesOrderDetailID]',
                '@qualified': '[Sales].[SalesOrderDetail]', '@where': predicate,
                '@order': '[ModifiedDate],[SalesOrderID],[SalesOrderDetailID]'})
            actual = db.execute(query).fetchall()
            count_expr = re.search(r"SET @sql=(N'SELECT @n=COUNT_BIG.*?);\nEXEC", sql, re.S)[1]
            count_query = concat_expression(count_expr, {'@qualified': '[Sales].[SalesOrderDetail]', '@where': predicate})
            # SQLite adapter only removes T-SQL output assignment/COUNT_BIG.
            count_query = count_query.replace('@n=COUNT_BIG(*)', 'COUNT(*)')
            self.assertEqual(db.execute(count_query).fetchone()[0], len(actual))
        return actual, [row for row in rows if lower < row <= upper]

    def test_sales_order_detail_equal_timestamp_upper_and_lower_edges(self):
        actual, expected = self.detail_window()
        self.assertEqual(actual, expected)
        self.assertEqual([(r[1], r[2]) for r in actual], [(100, 6), (101, 999), (102, 1), (102, 10)])

    def test_regression_suite_detects_inclusive_first_key_and_independent_key_mutations(self):
        mutations = [
            SQL.replace("N'('+QUOTENAME(@k1)+N'<'", "N'('+QUOTENAME(@k1)+N'<='"),
            SQL.replace("+QUOTENAME(@k2)+N'<='", "+QUOTENAME(@k2)+N'<'"),
            SQL.replace("SET @uk=N'('+QUOTENAME(@k1)+N'<'+CONVERT(nvarchar(20),@u1)\n"
                        "            +N' OR ('+QUOTENAME(@k1)+N'='+CONVERT(nvarchar(20),@u1)+N' AND '\n"
                        "            +QUOTENAME(@k2)+N'<='+CONVERT(nvarchar(20),@u2)+N'))'",
                        "SET @uk=N'('+QUOTENAME(@k1)+N'<='+CONVERT(nvarchar(20),@u1)"
                        "+N' AND '+QUOTENAME(@k2)+N'<='+CONVERT(nvarchar(20),@u2)+N')'")]
        for mutated in mutations:
            self.assertNotEqual(mutated, SQL)
            actual, expected = self.detail_window(mutated)
            self.assertNotEqual(actual, expected, 'The fixtures must detect this incorrect upper-bound implementation')

    def test_captured_maximum_is_one_real_tuple_not_independent_maxima(self):
        expression = re.search(r"SET @sql=(N'SELECT TOP \(1\).*?);\n    EXEC", SQL, re.S)[1]
        for composite in [False, True]:
            rendered = expression.replace("CASE WHEN @k2 IS NULL THEN N'' ELSE N', @b='+QUOTENAME(@k2) END",
                                          "N', @b=[SalesOrderDetailID]'" if composite else "N''")
            rendered = rendered.replace("CASE WHEN @k2 IS NULL THEN N'' ELSE N','+QUOTENAME(@k2)+N' DESC' END",
                                        "N',[SalesOrderDetailID] DESC'" if composite else "N''")
            query = concat_expression(rendered, {'@k1': 'SalesOrderID', '@qualified': 'source', '@where': '1=1'})
            query = query.replace('SELECT TOP (1)', 'SELECT').replace('@u=CONVERT(datetime2(7),[ModifiedDate])', '[ModifiedDate]')
            query = query.replace('@a=', '').replace('@b=', '').rstrip(';') + ' LIMIT 1;'
            with sqlite3.connect(':memory:') as db:
                db.execute('CREATE TABLE source (ModifiedDate TEXT, SalesOrderID INT, SalesOrderDetailID INT)')
                rows = [('2026-10-14T12:00:00.1234567', 101, 999),
                        ('2026-10-14T12:00:00.1234567', 102, 10),
                        ('2026-10-14T12:00:00.1234567', 102, 9)]
                db.executemany('INSERT INTO source VALUES (?,?,?)', rows)
                self.assertEqual(db.execute(query).fetchone(), max(row[:3 if composite else 2] for row in rows))
                db.execute('INSERT INTO source VALUES (?,?,?)', ('2026-10-14T12:00:00.1234568', 1, 1))
                self.assertEqual(db.execute(query).fetchone(), ('2026-10-14T12:00:00.1234568', 1, 1)[:3 if composite else 2])

    def test_actual_predicate_fragments_match_tuple_oracle(self):
        rng = random.Random(19)
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE source (ModifiedDate INTEGER, PK1 INTEGER, PK2 INTEGER)')
            rows = list(itertools.product(range(3), range(1, 4), range(1, 4)))
            db.executemany('INSERT INTO source VALUES (?,?,?)', rows)
            for composite in [False, True]:
                width = 3 if composite else 2
                windows = [(rows[0][:width], rows[-1][:width]), ((1, 1, 2)[:width], (1, 2, 1)[:width]),
                           ((1, 2, 2)[:width], (1, 2, 2)[:width])]
                windows += [tuple(sorted((rng.choice(rows)[:width], rng.choice(rows)[:width]))) for _ in range(50)]
                for lower, upper in windows:
                    predicate = rendered_predicate(lower, upper, composite)
                    actual = db.execute('SELECT * FROM source WHERE ' + predicate + ' ORDER BY ModifiedDate,PK1,PK2').fetchall()
                    expected = [row for row in rows if lower < row[:width] <= upper]
                    self.assertEqual(actual, expected, (lower, upper, predicate))


if __name__ == '__main__':
    unittest.main()
