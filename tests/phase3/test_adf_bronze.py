"""Offline checks only. No SQL Server, Azure, network, or credentials.

Predicate tests interpret the actual planner's concatenation fragments and execute
those comparisons against synthetic SQLite rows; they do not compile T-SQL.
"""
import importlib.util
import itertools
import json
import random
import re
import sqlite3
import unittest
from pathlib import Path

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


def rendered_predicate(lower, upper, composite):
    variables = {'@k1': 'PK1', '@k2': 'PK2', '@l1': lower[1], '@u1': upper[1],
                 '@l2': lower[2] if composite else 0, '@u2': upper[2] if composite else 0,
                 '@wm': '[ModifiedDate]', '@ld': str(lower[0]), '@ud': str(upper[0])}
    for var in ['lk', 'uk']:
        declaration = re.search(r'DECLARE @' + var + r' nvarchar\(600\)=(.*?);', SQL, re.S)[1]
        variables['@' + var] = concat_expression(declaration, variables)
        if composite:
            compound = re.search(r'IF @k2 IS NOT NULL SET @' + var + r'=(.*?);', SQL, re.S)[1]
            variables['@' + var] = concat_expression(compound, variables)
    lower_expr = re.search(r"SET @where=(N'\('\+@wm.*?);", SQL, re.S)[1]
    variables['@where'] = concat_expression(lower_expr, variables)
    upper_expr = re.search(r'SET @where=(@where\+.*?);', SQL, re.S)[1]
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
            self.assertNotIn('variables', properties)
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
        self.assertFalse(types & {'Script', 'SqlServerStoredProcedure', 'WebActivity', 'Delete'})
        code = re.sub(r"--[^\n]*|'(?:''|[^'])*'", '', SQL)
        self.assertIsNone(re.search(r'\b(UPDATE|DELETE|MERGE|TRUNCATE|DROP|GRANT|CREATE|ALTER)\b', code, re.I))
        self.assertNotRegex(code, r'INSERT\s+ingestion\.')
        self.assertIn('USE [AdventureWorks];', SQL)

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
        match = re.fullmatch(r"@concat\('((?:''|[^'])*)',string\(pipeline\(\).parameters.config_id\),'((?:''|[^'])*)'\)", plan, re.S)
        self.assertIsNotNone(match)
        compiled = match[1].replace("''", "'") + '123' + match[2].replace("''", "'")
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


class CursorPredicateTests(unittest.TestCase):
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
