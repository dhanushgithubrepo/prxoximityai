"""SendGrid email integration for sending actual emails."""
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

@dataclass
class EmailResult:
    """Result of an email send operation."""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    status_code: Optional[int] = None

class SendGridEmailer:
    """SendGrid email client for sending customer emails."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("SENDGRID_API_KEY")
        self.enabled = bool(self.api_key)
        self.from_email = os.getenv("FROM_EMAIL", "noreply@proximity.ai")
        self.sg = None
        
        if self.enabled:
            try:
                from sendgrid import SendGridAPIClient
                self.sg = SendGridAPIClient(self.api_key)
                print("SendGrid initialized successfully")
            except Exception as e:
                print(f"SendGrid init error: {e}")
                self.enabled = False
    
    def send_email(self, to_email: str, subject: str, body: str, 
                   html_body: Optional[str] = None) -> EmailResult:
        """Send a single email."""
        if not self.enabled:
            return EmailResult(
                success=False, 
                error="SendGrid not configured. Set SENDGRID_API_KEY env var."
            )
        
        try:
            from sendgrid.helpers.mail import Mail, Email, Content, HtmlContent
            
            message = Mail(
                from_email=Email(self.from_email),
                to_emails=Email(to_email),
                subject=subject,
                plain_text_content=Content("text/plain", body),
                html_content=HtmlContent(html_body or f"<pre>{body}</pre>")
            )
            
            response = self.sg.send(message)
            
            return EmailResult(
                success=response.status_code in [200, 201, 202],
                message_id=response.headers.get('X-Message-Id'),
                status_code=response.status_code
            )
            
        except Exception as e:
            return EmailResult(success=False, error=str(e))
    
    def send_bulk_emails(self, emails: List[Dict[str, str]]) -> Dict[str, Any]:
        """Send multiple emails in bulk."""
        if not self.enabled:
            return {
                "total": len(emails),
                "sent": 0,
                "failed": len(emails),
                "errors": ["SendGrid not configured"]
            }
        
        results = {
            "total": len(emails),
            "sent": 0,
            "failed": 0,
            "errors": [],
            "message_ids": []
        }
        
        for email_data in emails:
            result = self.send_email(
                to_email=email_data.get("to", ""),
                subject=email_data.get("subject", ""),
                body=email_data.get("body", "")
            )
            
            if result.success:
                results["sent"] += 1
                if result.message_id:
                    results["message_ids"].append(result.message_id)
            else:
                results["failed"] += 1
                results["errors"].append(result.error)
        
        return results
    
    def get_status(self) -> Dict[str, Any]:
        """Get SendGrid configuration status."""
        return {
            "enabled": self.enabled,
            "from_email": self.from_email,
            "configured": bool(self.api_key),
            "message": "Ready to send emails" if self.enabled else "Set SENDGRID_API_KEY to enable"
        }

# Global emailer instance
emailer = SendGridEmailer()

def send_customer_email(customer_email: str, subject: str, body: str) -> EmailResult:
    """Send email to a customer using the global emailer."""
    return emailer.send_email(customer_email, subject, body)

def send_agent_action_emails(actions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send emails for all agent actions."""
    emails = []
    for action in actions:
        if action.get("email_subject") and action.get("email_body"):
            emails.append({
                "to": action.get("customer_email"),
                "subject": action.get("email_subject"),
                "body": action.get("email_body")
            })
    
    return emailer.send_bulk_emails(emails)
