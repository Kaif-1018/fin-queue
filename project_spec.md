# Project Specification: Async Job Processing Platform

## Architecture
- **Backend:** FastAPI (REST API + WebSockets for real-time status updates).
- **Database:** PostgreSQL (persistent storage for job history, metadata, and status records).
- **ORM & Migrations:** SQLAlchemy / SQLModel + Alembic.
- **Message Broker:** Redis (Celery task queue broker and WebSocket Pub/Sub).
- **Task Queue/Workers:** Celery.
- **Frontend:** Minimal React SPA (job submission + live status dashboard).
- **Infrastructure:** Docker Compose (FastAPI, Celery Worker, PostgreSQL, Redis, React).

## Data Model (Jobs Table)
- `id`: UUID (Primary Key)
- `job_type`: String (e.g., "document_extraction")
- `status`: Enum ("QUEUED", "PROCESSING", "COMPLETED", "FAILED")
- `payload`: JSONB (job parameters/input data)
- `result`: JSONB (final output/metadata)
- `created_at`: Timestamp
- `updated_at`: Timestamp