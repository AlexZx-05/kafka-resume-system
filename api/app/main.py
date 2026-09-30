from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from uuid import uuid4
from datetime import datetime, timezone

from app.schemas import ResumeUploadRequest
from app.producer import publish_resume_event


app = FastAPI(title="Kafka Resume Processing System")


@app.get("/")
def health_check():
    return {
        "status": "ok",
        "service": "resume-api"
    }


@app.post("/resumes", status_code=status.HTTP_202_ACCEPTED)
def upload_resume(request: ResumeUploadRequest):

    resume_id = str(uuid4())
    event_id = str(uuid4())

    event = {
        "event_id": event_id,
        "event_type": "resume-uploaded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "candidate_id": request.candidate_id,
        "resume_id": resume_id,
        "data": {
            "filename": request.filename
        }
    }

    publish_resume_event(event)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "message": "Resume accepted for processing",
            "resume_id": resume_id,
            "event_id": event_id
        }
    )