"""Advanced agent features: parallel execution, conditional routing, and memory."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

class ChannelType(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WHATSAPP = "whatsapp"

@dataclass
class AgentMemory:
    """Memory for tracking customer interactions across sessions."""
    customer_email: str
    previous_actions: List[Dict[str, Any]] = field(default_factory=list)
    email_opens: int = 0
    email_clicks: int = 0
    last_contact: Optional[datetime] = None
    preferred_channel: ChannelType = ChannelType.EMAIL
    escalation_count: int = 0
    
    def record_action(self, action_type: str, channel: ChannelType, success: bool = True):
        """Record an action taken for this customer."""
        self.previous_actions.append({
            "action_type": action_type,
            "channel": channel.value,
            "timestamp": datetime.now().isoformat(),
            "success": success
        })
        self.last_contact = datetime.now()
        
        # Keep only last 10 actions
        if len(self.previous_actions) > 10:
            self.previous_actions = self.previous_actions[-10:]
    
    def should_escalate(self) -> bool:
        """Determine if we should escalate to a different channel."""
        # Escalate if last 2 emails weren't opened
        recent_emails = [a for a in self.previous_actions[-3:] 
                        if a.get("channel") == "email"]
        if len(recent_emails) >= 2:
            failed_emails = [a for a in recent_emails if not a.get("success", True)]
            if len(failed_emails) >= 2:
                return True
        return False
    
    def get_next_channel(self) -> ChannelType:
        """Get the next channel to try based on history."""
        if self.should_escalate() and self.escalation_count < 2:
            self.escalation_count += 1
            # Escalation chain: Email -> SMS -> WhatsApp
            if self.preferred_channel == ChannelType.EMAIL:
                return ChannelType.SMS
            elif self.preferred_channel == ChannelType.SMS:
                return ChannelType.WHATSAPP
        return self.preferred_channel

# Global memory store (in production, use Redis)
agent_memories: Dict[str, AgentMemory] = {}

def get_or_create_memory(customer_email: str) -> AgentMemory:
    """Get existing memory or create new one."""
    if customer_email not in agent_memories:
        agent_memories[customer_email] = AgentMemory(customer_email=customer_email)
    return agent_memories[customer_email]

def process_customer_parallel(customer: Dict[str, Any], 
                              executor: ThreadPoolExecutor) -> Optional[Dict[str, Any]]:
    """Process a single customer with parallel execution."""
    # Get or create memory for this customer
    memory = get_or_create_memory(customer.get("email", ""))
    
    # Check if we need to escalate channel
    channel = memory.get_next_channel()
    should_escalate = channel != ChannelType.EMAIL
    
    # Determine action based on tier and memory
    tier = customer.get("tier", "")
    churn_risk = customer.get("churn_risk", 0)
    segment = customer.get("segment", "")
    
    action_type = "monitor_only"
    if tier == "At-Risk" and churn_risk >= 80:
        action_type = "email_discount"
    elif segment == "VIP Inactive":
        action_type = "vip_winback"
    elif tier == "At-Risk" or churn_risk >= 70:
        action_type = "email_reengagement"
    
    # Override with escalated channel if needed
    if should_escalate:
        action_type = f"{channel.value}_escalation"
    
    # Record this action in memory
    memory.record_action(action_type, channel)
    
    return {
        "customer_email": customer.get("email"),
        "action_type": action_type,
        "channel": channel.value,
        "escalated": should_escalate,
        "previous_actions_count": len(memory.previous_actions),
        "memory": {
            "email_opens": memory.email_opens,
            "email_clicks": memory.email_clicks,
            "last_contact": memory.last_contact.isoformat() if memory.last_contact else None,
            "escalation_count": memory.escalation_count
        }
    }

def run_parallel_agent_workflow(customers: List[Dict[str, Any]], 
                                 max_workers: int = 4) -> Dict[str, Any]:
    """Run agent workflow in parallel for multiple customers."""
    import time
    start_time = time.time()
    
    # Filter customers that need routing
    routed_customers = [
        c for c in customers 
        if c.get("tier") == "At-Risk" or 
           c.get("churn_risk", 0) >= 70 or 
           c.get("segment") == "VIP Inactive"
    ]
    
    # Process in parallel using ThreadPoolExecutor
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_customer_parallel, c, executor) 
                  for c in routed_customers]
        
        for future in futures:
            try:
                result = future.result(timeout=5)  # 5 second timeout per customer
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Error processing customer: {e}")
    
    # Calculate statistics
    escalated_count = sum(1 for r in results if r.get("escalated", False))
    channel_breakdown = {}
    for r in results:
        ch = r.get("channel", "unknown")
        channel_breakdown[ch] = channel_breakdown.get(ch, 0) + 1
    
    latency_ms = int((time.time() - start_time) * 1000)
    
    return {
        "actions": results,
        "metadata": {
            "total_customers": len(customers),
            "routed_customers": len(routed_customers),
            "processed": len(results),
            "escalated": escalated_count,
            "channel_breakdown": channel_breakdown,
            "parallel_workers": max_workers,
            "latency_ms": latency_ms,
            "memory_enabled": True
        }
    }

def get_customer_memory(customer_email: str) -> Optional[Dict[str, Any]]:
    """Get customer memory for inspection."""
    if customer_email in agent_memories:
        mem = agent_memories[customer_email]
        return {
            "customer_email": mem.customer_email,
            "previous_actions": mem.previous_actions,
            "email_opens": mem.email_opens,
            "email_clicks": mem.email_clicks,
            "last_contact": mem.last_contact.isoformat() if mem.last_contact else None,
            "preferred_channel": mem.preferred_channel.value,
            "escalation_count": mem.escalation_count,
            "should_escalate": mem.should_escalate()
        }
    return None

def get_all_memories() -> Dict[str, Any]:
    """Get all customer memories summary."""
    return {
        "total_memories": len(agent_memories),
        "customers": [
            {
                "email": email,
                "actions_count": len(mem.previous_actions),
                "last_contact": mem.last_contact.isoformat() if mem.last_contact else None,
                "escalation_count": mem.escalation_count
            }
            for email, mem in agent_memories.items()
        ]
    }

def clear_all_memories():
    """Clear all agent memories (for testing)."""
    agent_memories.clear()
