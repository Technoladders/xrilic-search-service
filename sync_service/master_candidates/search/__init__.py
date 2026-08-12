"""
sync_service/master_candidates/search/

Query-planning layer for /mc/search and /mc/search_v2. search_api.py stays
the thin FastAPI-facing orchestrator; everything that decides WHICH
candidates match (MUST/NICE/EXCLUDE, keyword Boolean parsing/evaluation,
ranking) lives here so it can be unit-tested without FastAPI/httpx/Typesense.
"""
