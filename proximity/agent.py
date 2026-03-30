import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field

# LangSmith tracing
from langsmith_tracer import tracer

class ActionType(str, Enum):
    EMAIL_DISCOUNT = "email_discount"
    EMAIL_REENGAGEMENT = "email_reengagement"
    VIP_WINBACK = "vip_winback"
    MONITOR_ONLY = "monitor_only"

class CustomerAction(BaseModel):
    customer_id: str
    customer_email: str
    customer_name: str
    tier: str
    segment: str
    churn_risk: int
    rfm_score: str
    action_type: ActionType
    action_details: Dict[str, Any]
    discount_code: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    priority: int = Field(default=0, description="Higher number = higher priority")

class AgentState(BaseModel):
    customers: List[Dict[str, Any]]
    actions: List[CustomerAction] = Field(default_factory=list)
    discount_codes: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

def generate_discount_code() -> str:
    """Generate a unique discount code."""
    prefix = "SAVE"
    suffix = uuid.uuid4().hex[:6].upper()
    return f"{prefix}{suffix}"

def should_route_to_agent(customer: Dict[str, Any]) -> bool:
    """Determine if a customer needs agent-level routing."""
    tier = customer.get("tier", "")
    churn_risk = customer.get("churn_risk", 0)
    segment = customer.get("segment", "")
    return (
        tier == "At-Risk" or
        churn_risk >= 40 or
        segment == "VIP Inactive"
    )

def route_customer(customer: Dict[str, Any]) -> ActionType:
    """Route customer to appropriate action type."""
    tier = customer.get("tier", "")
    segment = customer.get("segment", "")
    churn_risk = customer.get("churn_risk", 0)
    
    if tier == "At-Risk" and churn_risk >= 80:
        return ActionType.EMAIL_DISCOUNT
    elif segment == "VIP Inactive":
        return ActionType.VIP_WINBACK
    elif tier == "At-Risk" or churn_risk >= 70:
        return ActionType.EMAIL_REENGAGEMENT
    else:
        return ActionType.MONITOR_ONLY

def build_action_details(action_type: ActionType, customer: Dict[str, Any]) -> Dict[str, Any]:
    """Build action-specific details."""
    details = {"reason": "Churn prevention"}
    
    if action_type == ActionType.EMAIL_DISCOUNT:
        details.update({
            "discount_type": "percentage",
            "discount_value": 20,
            "expiry_days": 7,
            "min_order_value": 50,
            "usage_limit": 1
        })
    elif action_type == ActionType.VIP_WINBACK:
        details.update({
            "discount_type": "percentage",
            "discount_value": 25,
            "expiry_days": 14,
            "min_order_value": 100,
            "usage_limit": 1,
            "personal_touch": True
        })
    elif action_type == ActionType.EMAIL_REENGAGEMENT:
        details.update({
            "discount_type": "percentage",
            "discount_value": 15,
            "expiry_days": 10,
            "min_order_value": 30,
            "usage_limit": 1
        })
    
    return details

def generate_email_content(action_type: ActionType, customer: Dict[str, Any], discount_code: str) -> tuple[str, str]:
    """Generate email subject and body."""
    name = customer.get("name", "Customer")
    tier = customer.get("tier", "")
    segment = customer.get("segment", "")
    
    if action_type == ActionType.EMAIL_DISCOUNT:
        subject = f"🔥 20% OFF – We miss you, {name}!"
        body = f"""Hi {name},

We noticed you haven't visited in a while and we want you back!

Use code **{discount_code}** for 20% off your next order (min $50).

This offer expires in 7 days. Don't miss out!

Shop now → [Your Store Link]

Best,
The Team"""
    
    elif action_type == ActionType.VIP_WINBACK:
        subject = f"✨ Exclusive VIP Offer – Just for You, {name}"
        body = f"""Dear {name},

As a valued VIP, we miss having you with us!

Enjoy an exclusive 25% off with code **{discount_code}** (min $100).

Valid for 14 days. We hope to see you again soon!

Shop VIP → [Your Store Link]

Warmly,
The VIP Team"""
    
    elif action_type == ActionType.EMAIL_REENGAGEMENT:
        subject = f"👋 A little something from us to you, {name}"
        body = f"""Hi {name},

It's been a while! Here's 15% off to welcome you back.

Code: **{discount_code}** (min $30, expires in 10 days)

We've added new items we think you'll love!

Browse → [Your Store Link]

Cheers,
The Team"""
    
    else:
        subject = "Just checking in"
        body = f"Hi {name}, we hope you're doing well!"
    
    return subject, body

def calculate_priority(action_type: ActionType, churn_risk: int) -> int:
    """Calculate action priority (higher = more urgent)."""
    base = {
        ActionType.EMAIL_DISCOUNT: 80,
        ActionType.VIP_WINBACK: 90,
        ActionType.EMAIL_REENGAGEMENT: 60,
        ActionType.MONITOR_ONLY: 10
    }
    return base.get(action_type, 0) + (churn_risk // 10)

def run_agent_workflow(customers: List[Dict[str, Any]]) -> AgentState:
    """Run LangGraph-style agent workflow on customers with LangSmith tracing."""
    state = AgentState(customers=customers)
    import time
    workflow_start = time.time()
    
    for customer in customers:
        if not should_route_to_agent(customer):
            continue
        
        # Start tracing this customer action
        action_start = time.time()
        
        # Route action
        action_type = route_customer(customer)
        
        # Generate discount if needed
        discount_code = None
        if action_type in [ActionType.EMAIL_DISCOUNT, ActionType.VIP_WINBACK, ActionType.EMAIL_REENGAGEMENT]:
            discount_code = generate_discount_code()
            state.discount_codes.append(discount_code)
        
        # Build action details
        action_details = build_action_details(action_type, customer)
        
        # Generate email content
        email_subject, email_body = generate_email_content(action_type, customer, discount_code or "")
        
        # Schedule (immediate for demo)
        scheduled_at = datetime.now() + timedelta(minutes=15)
        
        # Calculate priority
        priority = calculate_priority(action_type, customer.get("churn_risk", 0))
        
        # Create action
        action = CustomerAction(
            customer_id=str(customer.get("customer_id", customer.get("email", ""))),
            customer_email=customer.get("email", ""),
            customer_name=customer.get("name", ""),
            tier=customer.get("tier", ""),
            segment=customer.get("segment", ""),
            churn_risk=customer.get("churn_risk", 0),
            rfm_score=customer.get("rfm_score", ""),
            action_type=action_type,
            action_details=action_details,
            discount_code=discount_code,
            email_subject=email_subject,
            email_body=email_body,
            scheduled_at=scheduled_at,
            priority=priority
        )
        
        state.actions.append(action)
        
        # Trace the action
        latency_ms = int((time.time() - action_start) * 1000)
        tracer.trace_agent_action(
            customer_email=customer.get("email", ""),
            tier=customer.get("tier", ""),
            segment=customer.get("segment", ""),
            action_type=action_type.value,
            discount_code=discount_code or "",
            priority=priority
        )
    
    # Sort actions by priority (descending)
    state.actions.sort(key=lambda a: a.priority, reverse=True)
    
    # Add metadata
    total_latency_ms = int((time.time() - workflow_start) * 1000)
    state.metadata.update({
        "total_customers": len(customers),
        "routed_customers": len(state.actions),
        "generated_discounts": len(state.discount_codes),
        "run_at": datetime.now().isoformat(),
        "workflow_latency_ms": total_latency_ms,
        "traced_actions": len(tracer.traces)
    })
    
    return state
