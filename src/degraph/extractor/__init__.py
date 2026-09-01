"""DEGraph extractor package.

Entry points:
    from degraph.extractor.assembler import extract_repo   # full repo → Graph
    from degraph.extractor.file_extractor import FileExtractor  # single file
"""

from degraph.extractor.assembler import extract_repo

__all__ = ["extract_repo"]
