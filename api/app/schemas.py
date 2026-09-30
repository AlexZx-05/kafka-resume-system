from pydantic import BaseModel


class ResumeUploadRequest(BaseModel):
    candidate_id: str
    filename: str