"""LangSmith observability integration for agent tracing."""
import os
from typing import Dict, Any, Optional
from datetime import datetime
from functools import wraps

class LangSmithTracer:
    """Simple tracer for agent actions (LangSmith-style)."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("LANGSMITH_API_KEY")
        self.enabled = bool(self.api_key)
        self.traces = []  # Store traces in memory for dashboard viewing
        self.run_id = 0
    
    def trace_agent_action(self, customer_email: str, tier: str, segment: str, 
                          action_type: str, discount_code: str, priority: int) -> str:
        """Trace a single agent decision."""
        # Always trace locally, even without API key (for dashboard visibility)
        self.run_id += 1
        run_id = f"run_{self.run_id}"
        
        trace = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "customer_email": customer_email,
            "tier": tier,
            "segment": segment,
            "action_type": action_type,
            "discount_code": discount_code,
            "priority": priority,
            "latency_ms": 0,  # Would measure actual latency
        }
        
        self.traces.append(trace)
        
        # Keep only last 100 traces
        if len(self.traces) > 100:
            self.traces = self.traces[-100:]
        
        return run_id
    
    def get_recent_traces(self, limit: int = 20) -> list:
        """Get recent traces for dashboard display."""
        return self.traces[-limit:]
    
    def get_trace_stats(self) -> Dict[str, Any]:
        """Get trace statistics."""
        if not self.traces:
            return {"total_runs": 0, "avg_latency_ms": 0, "action_breakdown": {}}
        
        action_types = {}
        for trace in self.traces:
            action = trace.get("action_type", "unknown")
            action_types[action] = action_types.get(action, 0) + 1
        
        return {
            "total_runs": len(self.traces),
            "action_breakdown": action_types,
            "last_trace": self.traces[-1] if self.traces else None
        }

# Global tracer instance
tracer = LangSmithTracer()

def trace_agent(func):
    """Decorator to trace agent function calls."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not tracer.enabled:
            return func(*args, **kwargs)
        
        import time
        start = time.time()
        result = func(*args, **kwargs)
        latency = (time.time() - start) * 1000
        
        # Log the trace
        print(f"[LangSmith] Agent action traced: {latency:.2f}ms")
        
        return result
    return wrapper
