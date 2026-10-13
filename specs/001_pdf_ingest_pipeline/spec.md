# Feature Specification: PDF Ingest Pipeline

**Feature Branch**: [pdf-ingest-pipeline]

**Created**: 2026-10-01

**Status**: Draft

**Input**: User description: "Create a feature specification from specs/001_pdf_ingest_pipeline/1_requirements.md"

## Clarifications

### Session 2026-10-01

- Q: What is the expected maximum number of PDF documents to be processed daily by this pipeline? → A: 5/sec on 9th gen Intel, 16GB RAM
- Q: Are there any security or privacy considerations for the PDF content being processed? → A: All pdf's are publicly available and therefore not sensitive

## User Scenarios & Testing

### User Story 1 - Extract PDF Documents from URLs (Priority: P1)

System operators need to extract text content from PDF documents available at URLs stored in the database. The system must fetch these URLs, download the PDF files, extract their text content, and store the results back in the database for later processing by LLM summarization.

**Why this priority**: This is the core functionality of the PDF ingest pipeline. Without successful extraction, no downstream processing (summarization, analysis) can occur. This delivers the primary value of making PDF content accessible for LLM processing.

**Independent Test**: Can be fully tested by running the pipeline against a known set of PDF URLs and verifying that text content is successfully extracted and stored in the database.

**Acceptance Scenarios**:

1. **Given** a database containing PDF URLs, **When** the pipeline runs, **Then** all text-based PDFs are successfully extracted and stored
2. **Given** a PDF URL that returns a valid text-based PDF, **When** processed by the pipeline, **Then** the extracted text is stored in the database with reference to the original file ID
3. **Given** a PDF URL that contains scanned images, **When** processed by the pipeline, **Then** it is logged as failed with an appropriate error code and excluded from re-attempts

---

### User Story 2 - Handle Rate Limiting Gracefully (Priority: P2)

The system must respect rate limits when fetching PDFs from ft.dk to avoid being blocked or causing excessive load on the source server.

**Why this priority**: Rate limiting ensures the pipeline can run continuously without being blocked by the source. It is critical for production reliability.

**Independent Test**: Can be tested by configuring a rate limit and verifying that the pipeline does not exceed it under load.

**Acceptance Scenarios**:

1. **Given** a configured rate limit of 5 requests per second, **When** processing multiple URLs, **Then** the pipeline never exceeds this limit
2. **Given** a burst of URLs to process, **When** the rate limit is reached, **Then** requests are queued and processed at the configured rate

---

### User Story 3 - Recover from Failures Idempotently (Priority: P2)

The system must be able to recover from failures (network errors, database errors) by restarting the process without duplicating work or losing state.

**Why this priority**: Idempotency ensures that failures do not corrupt data or require manual intervention. This is essential for production reliability.

**Independent Test**: Can be tested by simulating failures (network outages, database disconnects) and verifying that restarting the pipeline resumes from the correct state without duplicates.

**Acceptance Scenarios**:

1. **Given** a pipeline run that fails mid-process, **When** restarted, **Then** it resumes from where it left off without reprocessing already-completed URLs
2. **Given** a database connection failure during processing, **When** the connection is restored and pipeline restarts, **Then** state is recovered and processing continues

---

### Edge Cases

- What happens when the database is unavailable during pipeline startup? Pipeline logs error and exits with warning; can be restarted when database is available
- What happens when a PDF URL returns a non-PDF file? File is logged as failed with appropriate error code and excluded from re-attempts
- What happens when the same URL is processed twice? Idempotency ensures it is only processed once; duplicate attempts are detected and skipped
- How does system handle network timeouts? Exponential backoff is applied with warning logs
- How does system handle PDFs with hybrid content (text + images)? Logged as failed with specific error type and excluded from re-attempts

## Requirements

### Functional Requirements

- **FR-001**: System MUST extract text content from text-based PDF documents fetched from URLs
- **FR-002**: System MUST store extracted PDF content in the database as whole files, not chunked
- **FR-003**: System MUST fetch PDF URLs from the database in prioritized order (newest first by latest date modified)
- **FR-004**: System MUST enforce rate limiting of 5 requests per second or less (configurable default)
- **FR-005**: System MUST log successful, failed, and cancelled extractions idempotently with reference to file ID, timestamp, and error type
- **FR-006**: System MUST re-attempt failed or cancelled loads by including them in the prioritized URL list
- **FR-007**: System MUST detect and log PDFs containing scanned images or hybrid content with a specific error type
- **FR-008**: System MUST exclude image-containing PDFs from re-attempts
- **FR-009**: System MUST store processed documents in a format that facilitates later summarization by LLMs
- **FR-010**: System MUST be idempotent - processing the same URL multiple times must not create duplicate entries
- **FR-011**: System MUST implement exponential backoff for network/API failures
- **FR-012**: System MUST provide an architecture diagram that is human-readable
- **FR-013**: System MUST document design decisions and trade-offs in a design document

### Key Entities

- **PDF URL**: A URL pointing to a PDF document on ft.dk; contains metadata like file ID, date modified; source of truth for what to process
- **Extracted Document**: The text content extracted from a PDF; stored as a whole file in the database; ready for LLM summarization
- **Processing Log**: Record of each extraction attempt; contains file ID, timestamp, status (success/fail/cancelled), error type; ensures idempotency
- **Error Type**: Classification of failure modes; includes network errors, PDF anomalies (images/hybrid), rate limit exceeded; used for filtering and re-attempt logic

## Success Criteria

### Measurable Outcomes

- **SC-001**: Pipeline successfully extracts text from 99% of PDFs in target corpus without errors
- **SC-002**: URLs are fetched and processed in prioritized order (newest first) with no out-of-order processing
- **SC-003**: Rate limiting enforces less than or equal to 5 requests per second under load
- **SC-004**: All successful, failed, and cancelled extractions are logged with file ID, timestamp, and error type
- **SC-005**: Failed or cancelled loads are automatically re-attempted through prioritized URL list inclusion
- **SC-006**: PDFs containing scanned images or hybrid content are identified and excluded from re-attempts
- **SC-007**: Processed documents are stored in a format that enables efficient LLM summarization
- **SC-008**: Architecture diagram is available and understandable to technical stakeholders
- **SC-009**: Design document clearly logs all design decisions and their trade-offs
- **SC-010**: Pipeline achieves 5 requests per second throughput on 9th generation Intel CPU with 16GB RAM hardware

## Assumptions

- PDF documents are text-based only (no OCR required for the target corpus)
- All PDF URLs are sourced from the ft.dk domain
- All PDFs are publicly available and therefore contain no sensitive data
- URLs to process are stored in the database with metadata including file ID and date modified
- The database (DuckDB) is available and accessible during pipeline execution
- Network connectivity to ft.dk is generally reliable with occasional transient failures
- Rate limiting of 5 requests per second is acceptable for ft.dk
- Processed documents will be consumed by LLM summarization workflows
- No ORM layer is needed; direct database operations are acceptable
- The system has sufficient storage for storing whole PDF files as text
- Pipeline runs on a single machine with 9th generation Intel CPU and 16GB RAM, targeting 5 requests per second throughput
