"""Read committed evidence without fallback, truncation or identity substitution."""
from .contracts import require,strict_json


def load_committed_evidence(receipt_file,parent):
    # Keep separate lines so safe diagnostics distinguish open, read and parse.
    with open(receipt_file,encoding='utf-8') as stream:
        text=stream.read()
    bundle=strict_json(text)
    # Valid JSON may be null, an array, or an object without this required key.
    # Previously those cases leaked TypeError/KeyError into RUNTIME_FAILED.
    require(isinstance(bundle,dict),'RECEIPT_OBJECT_REQUIRED')
    require(isinstance(bundle.get('parent_run_id'),str),'RECEIPT_PARENT_REQUIRED')
    require(bundle['parent_run_id']==parent,'REQUESTED_PARENT_MISMATCH')
    # Full hashes/11-table validation remains in pipeline.run, before publication.
    return bundle
