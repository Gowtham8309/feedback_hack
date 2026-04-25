from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class MonthlyTopicInput(BaseModel):
    month: str = Field(..., description="Format: YYYY-MM")
    district: str
    institute_name: str
    trade_name: str
    year: int
    semester: int
    subject_1: str
    topic_1: str
    subject_2: str
    topic_2: str
    subject_3: str
    topic_3: str


class GeneratedQuestion(BaseModel):
    subject: str
    topic: str
    question: str
    question_image: Optional[str] = None
    option_a: Optional[str] = None
    option_b: Optional[str] = None
    option_c: Optional[str] = None
    option_d: Optional[str] = None
    correct_option: Optional[str] = None
    answer_text: Optional[str] = None


class QuestionGenerationResponse(BaseModel):
    month: str
    district: str
    trade_name: str
    year: int
    semester: int
    questions: List[GeneratedQuestion]


class MonthlyFeedbackSubmission(BaseModel):
    month: str
    district: str
    trade_name: str
    year: int
    semester: int
    attendance_pct: float

    subject_1: str
    topic_1: str
    question_1: str
    topic_1_score: float

    subject_2: str
    topic_2: str
    question_2: str
    topic_2_score: float

    subject_3: str
    topic_3: str
    question_3: str
    topic_3_score: float

    teaching_score: float
    practical_score: float
    learning_score: float
    support_score: float
    safety_score: float

    comment_text: Optional[str] = ""


class SentimentResult(BaseModel):
    sentiment_label: str
    sentiment_score: float
    keywords: List[str] = []


class ProcessedFeedbackRecord(BaseModel):
    month: str
    district: str
    trade_name: str
    year: int
    semester: int
    attendance_pct: float

    subject_1: str
    topic_1: str
    question_1: str
    topic_1_score: float

    subject_2: str
    topic_2: str
    question_2: str
    topic_2_score: float

    subject_3: str
    topic_3: str
    question_3: str
    topic_3_score: float

    teaching_score: float
    practical_score: float
    learning_score: float
    support_score: float
    safety_score: float

    weak_topics: str
    comment_text: str
    sentiment_label: str
    sentiment_score: float


class DashboardSummary(BaseModel):
    total_submissions: int
    avg_teaching_score: float
    avg_practical_score: float
    avg_learning_score: float
    avg_support_score: float
    avg_safety_score: float
    avg_attendance_pct: float
    positive_count: int
    neutral_count: int
    negative_count: int
    top_weak_topics: List[str]


class CategoryParameterScore(BaseModel):
    parameter: str
    rating: Literal["Excellent", "Good", "Average", "Poor"]


class CategoryFeedbackSubmission(BaseModel):
    submitted_by_role: str
    source: str
    form_id: str
    form_title: Optional[str] = None
    basic_details: Dict[str, str] = Field(default_factory=dict)
    parameter_scores: List[CategoryParameterScore] = Field(default_factory=list)
    comment_text: Optional[str] = ""


class CategoryFeedbackRecord(BaseModel):
    submitted_at: str
    source: str
    form_id: str
    form_title: str
    basic_details: Dict[str, str]
    parameter_scores: List[CategoryParameterScore]
    comment_text: str
    excellent_count: int
    good_count: int
    average_count: int
    poor_count: int
    avg_rating_score: float


class UserRegisterRequest(BaseModel):
    full_name: Optional[str] = None
    username: str
    email: str
    password: str
    role: str
    assigned_trade: Optional[str] = None
    assigned_year: Optional[str] = None
    semester: Optional[str] = None
    district: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = "active"


class UserLoginRequest(BaseModel):
    username: str
    password: str


class UserAuthResponse(BaseModel):
    user_id: str
    username: str
    role: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    assigned_trade: Optional[str] = None
    assigned_year: Optional[int] = None
    semester: Optional[str] = None
    district: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    message: str


class RegisteredUserSummary(BaseModel):
    user_id: str
    username: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: str
    assigned_trade: Optional[str] = None
    assigned_year: Optional[str] = None
    semester: Optional[str] = None
    district: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = None
    created_by: Optional[str] = None
    created_at: str


class UserRegisterResponse(BaseModel):
    message: str
    user: RegisteredUserSummary


class RecentlyCreatedUserRow(BaseModel):
    user_id: str
    username: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: str
    assigned_trade: Optional[str] = None
    assigned_year: Optional[str] = None
    semester: Optional[str] = None
    district: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = None
    created_by: Optional[str] = None
    created_at: str


class TrainerQuestionSetRequest(BaseModel):
    username: str
    password: str
    month: str
    district: str
    institute_name: str
    trade_name: str
    year: int
    semester: int
    question_count: int = Field(default=3, ge=1, le=50)
    question_mode: Literal["theory", "practical", "both", "mcq"] = "both"
    subject_1: str
    topic_1: str
    subject_2: Optional[str] = None
    topic_2: Optional[str] = None
    subject_3: Optional[str] = None
    topic_3: Optional[str] = None
    question_pool: List[Dict[str, str]] = Field(default_factory=list)


class StudentLatestQuestionSetRequest(BaseModel):
    username: str
    password: str
    trade_name: str
    year: int
    semester: int


class StudentTechnicalQuestionResponse(BaseModel):
    subject: str
    topic: str
    question: str
    response_text: Optional[str] = ""
    selected_option: Optional[str] = None
    selected_option_text: Optional[str] = None
    confidence_score: float = Field(..., ge=1.0, le=5.0)


class StudentTechnicalFeedbackRequest(BaseModel):
    username: str
    password: str
    question_set_id: int
    month: str
    district: str
    institute_name: str
    trade_name: str
    year: int
    semester: int
    responses: List[StudentTechnicalQuestionResponse]


class QuestionBankIngestRequest(BaseModel):
    username: str
    password: str
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    default_source: Optional[str] = None
    default_trade: Optional[str] = None
    default_year_level: Optional[str] = None
    default_month: Optional[str] = None
