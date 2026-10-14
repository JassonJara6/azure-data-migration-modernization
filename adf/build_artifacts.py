"""Generate reviewable ADF authoring JSON. No Azure SDK, credentials or deployment.

The SQL batch is embedded as a Lookup expression; it is NOT installed on SQL Server.
Run from any directory. --check compares files without writing.
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def expr(value):
    return {'value': value, 'type': 'Expression'}


def ref(name, parameters):
    return {'referenceName': name, 'type': 'DatasetReference', 'parameters': parameters}


def after(name):
    return [{'activity': name, 'dependencyConditions': ['Succeeded']}]


def fail(name, message, code):
    return {'name': name, 'type': 'Fail', 'typeProperties': {'message': message, 'errorCode': code}}


def lookup(name, query, first, depends=None):
    return {'name': name, 'type': 'Lookup', 'dependsOn': depends or [],
            'policy': {'timeout': '0.00:20:00', 'retry': 0, 'secureInput': True, 'secureOutput': True},
            'typeProperties': {'source': {'type': 'SqlServerSource', 'sqlReaderQuery': query,
                                         'queryTimeout': '00:15:00', 'partitionOption': 'None'},
                               'dataset': ref('ds_adventureworks_sql', {'source_schema': 'ingestion', 'source_table': 'table_config'}),
                               'firstRowOnly': first}}


def artifacts():
    sql = (ROOT / 'sql/plan_bronze_batch.sql').read_text()
    before, after_id = sql.split('__CONFIG_ID__')
    quote = lambda s: "'" + s.replace("'", "''") + "'"
    plan_expr = expr('@concat(' + quote(before) + ',string(pipeline().parameters.config_id),' + quote(after_id) + ')')
    plan = lambda field: expr("@activity('PlanBatch').output.firstRow." + field)
    gate = {'name': 'AcknowledgeLandingOnly', 'type': 'IfCondition',
            'typeProperties': {'expression': expr('@pipeline().parameters.acknowledge_landing_only'),
                               'ifTrueActivities': [],
                               'ifFalseActivities': [fail('RequireAcknowledgement',
                                   'Landing only: no durable reconciliation, audit or watermark commit. Explicit acknowledgement required.',
                                   'LANDING_ONLY_ACK_REQUIRED')]}}
    parent = {'name': 'pl_adventureworks_bronze', 'properties': {
        'description': 'Landing-only prototype; intentional terminal failure prevents a committed-ingestion success signal.',
        'concurrency': 1,
        'parameters': {'acknowledge_landing_only': {'type': 'Bool', 'defaultValue': False}},
        'activities': [gate,
            lookup('ReadActiveConfigurations',
                   'USE [AdventureWorks]; SELECT config_id FROM ingestion.table_config WHERE is_active=1 ORDER BY config_id;',
                   False, after('AcknowledgeLandingOnly')),
            {'name': 'ForEachSource', 'type': 'ForEach', 'dependsOn': after('ReadActiveConfigurations'),
             'typeProperties': {'items': expr("@activity('ReadActiveConfigurations').output.value"),
                                'isSequential': True, 'activities': [
                 {'name': 'LandSource', 'type': 'ExecutePipeline',
                  'typeProperties': {'pipeline': {'referenceName': 'pl_adventureworks_bronze_table', 'type': 'PipelineReference'},
                                     'waitOnCompletion': True,
                                     'parameters': {'config_id': expr('@item().config_id'),
                                                    'acknowledge_landing_only': expr('@pipeline().parameters.acknowledge_landing_only'),
                                                    'ingestion_date': expr("@formatDateTime(pipeline().TriggerTime,'yyyy-MM-dd')"),
                                                    'parent_run_id': expr('@pipeline().RunId')}}}]}},
            dict(fail('WatermarkConfirmationBlocked',
                      'Landing finished, but ingestion is NOT committed. Independent destination reconciliation and durable audit/atomic watermark confirmation are not implemented. Do not promote these runs.',
                      'WATERMARK_CONFIRMATION_BLOCKED'), dependsOn=after('ForEachSource'))]}}
    copy = {'name': 'CopyToBronze', 'type': 'Copy', 'dependsOn': after('PlanBatch'),
            'policy': {'timeout': '0.02:00:00', 'retry': 0, 'secureInput': True, 'secureOutput': True},
            'inputs': [ref('ds_adventureworks_sql', {'source_schema': plan('source_schema'), 'source_table': plan('source_table')})],
            'outputs': [ref('ds_bronze_parquet', {'folder_path': expr(
                "@concat(activity('PlanBatch').output.firstRow.bronze_relative_path,'/load_type=',toLower(activity('PlanBatch').output.firstRow.load_type),'/ingestion_date=',pipeline().parameters.ingestion_date,'/run_id=',pipeline().RunId)")})],
            'typeProperties': {
                'source': {'type': 'SqlServerSource', 'sqlReaderQuery': plan('extract_query'),
                           'queryTimeout': '01:00:00', 'partitionOption': 'None',
                           'additionalColumns': [
                               {'name': '_ingested_at_utc', 'value': expr('@utcNow()')},
                               {'name': '_pipeline_run_id', 'value': expr('@pipeline().RunId')},
                               {'name': '_parent_pipeline_run_id', 'value': expr('@pipeline().parameters.parent_run_id')},
                               {'name': '_source_system', 'value': 'adventureworks'},
                               {'name': '_load_type', 'value': plan('load_type')},
                               {'name': '_watermark_lower', 'value': plan('watermark_lower')},
                               {'name': '_watermark_upper', 'value': plan('watermark_upper')},
                               {'name': '_watermark_lower_key_json', 'value': plan('lower_key_json')},
                               {'name': '_watermark_upper_key_json', 'value': plan('upper_key_json')} ]},
                'sink': {'type': 'ParquetSink', 'storeSettings': {'type': 'AzureBlobFSWriteSettings', 'maxConcurrentConnections': 1},
                         'formatSettings': {'type': 'ParquetWriteSettings'}},
                'enableStaging': False, 'parallelCopies': 1,
                'enableSkipIncompatibleRow': False,
                'translator': {'type': 'TabularTranslator', 'typeConversion': True,
                               'typeConversionSettings': {'allowDataTruncation': False, 'treatBooleanAsNumber': False}}}}
    child = {'name': 'pl_adventureworks_bronze_table', 'properties': {
        'description': 'Read-only batch planning and bounded copy. Copy success is not a committed ingestion.',
        'concurrency': 1,
        'parameters': {'config_id': {'type': 'Int'}, 'acknowledge_landing_only': {'type': 'Bool', 'defaultValue': False},
                       'ingestion_date': {'type': 'String'}, 'parent_run_id': {'type': 'String'}},
        'activities': [gate, lookup('PlanBatch', plan_expr, True, after('AcknowledgeLandingOnly')), copy,
            {'name': 'CheckCopyCounters', 'type': 'IfCondition', 'dependsOn': after('CopyToBronze'),
             'typeProperties': {'expression': expr(
                 "@and(equals(activity('CopyToBronze').output.rowsCopied,int(activity('PlanBatch').output.firstRow.expected_rows)),equals(activity('CopyToBronze').output.rowsRead,int(activity('PlanBatch').output.firstRow.expected_rows)))"),
                 'ifTrueActivities': [], 'ifFalseActivities': [fail('CopyCountMismatch',
                     'Source changed or copy counters differ from the bounded expected count. Landing is uncommitted; watermark unchanged.',
                     'COPY_COUNT_MISMATCH')]}}]}}
    dataset_sql = {'name': 'ds_adventureworks_sql', 'properties': {
        'linkedServiceName': {'referenceName': 'ls_sql_server', 'type': 'LinkedServiceReference'},
        'parameters': {'source_schema': {'type': 'String'}, 'source_table': {'type': 'String'}},
        'type': 'SqlServerTable', 'schema': [],
        'typeProperties': {'schema': expr('@dataset().source_schema'), 'table': expr('@dataset().source_table')}}}
    dataset_bronze = {'name': 'ds_bronze_parquet', 'properties': {
        'linkedServiceName': {'referenceName': 'ls_adls_gen2', 'type': 'LinkedServiceReference'},
        'parameters': {'folder_path': {'type': 'String'}}, 'type': 'Parquet', 'schema': [],
        'typeProperties': {'location': {'type': 'AzureBlobFSLocation', 'fileSystem': 'bronze',
                                       'folderPath': expr('@dataset().folder_path'), 'fileName': 'part-00000.parquet'},
                           'compressionCodec': 'snappy'}}}
    return {'pipeline/pl_adventureworks_bronze.json': parent,
            'pipeline/pl_adventureworks_bronze_table.json': child,
            'dataset/ds_adventureworks_sql.json': dataset_sql,
            'dataset/ds_bronze_parquet.json': dataset_bronze}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    for path, value in artifacts().items():
        text = json.dumps(value, indent=2) + '\n'
        target = ROOT / path
        if args.check:
            if not target.exists() or target.read_text() != text:
                raise SystemExit('Stale ADF artifact: ' + path)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
    print('ADF artifacts ' + ('match their sources' if args.check else 'generated locally (not published)'))


if __name__ == '__main__':
    main()
