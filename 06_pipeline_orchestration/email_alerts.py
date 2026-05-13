"""
EMAIL ALERTS - Sends notifications about pipeline status
This module sends email alerts when the pipeline succeeds or fails.
Configure email settings in config.py before using.

Author: Business Intelligence Platform
Date: 2026-05-12
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import EMAIL_SETTINGS

logger = logging.getLogger(__name__)


def send_alert(subject, message, is_error=True):
    """
    Send an email alert.
    
    Parameters:
        subject: Email subject line
        message: Email body text
        is_error: True for error alerts, False for success alerts
    
    Returns:
        True if email sent successfully, False otherwise
    """
    
    # Check if email settings are configured
    if EMAIL_SETTINGS.get('sender_email') == 'your_email@gmail.com':
        logger.warning("Email not configured. Update EMAIL_SETTINGS in config.py")
        print('-'*70)
        print("EMAIL ALERT (would have been sent):")
        print(f"Subject: {subject}")
        print(f"Message: {message}")
        print('-'*70)
        return False
    
    try:
        # Create email
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SETTINGS['sender_email']
        msg['To'] = EMAIL_SETTINGS['receiver_email']
        msg['Subject'] = subject
        
        # Add timestamp to message
        full_message = f"""
        Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        {message}
        
        ---
        Business Intelligence Platform
        Automated Pipeline Notification
        """
        
        msg.attach(MIMEText(full_message, 'plain'))
        
        # Send email
        server = smtplib.SMTP(EMAIL_SETTINGS['smtp_server'], EMAIL_SETTINGS['smtp_port'])
        server.starttls()
        server.login(EMAIL_SETTINGS['sender_email'], EMAIL_SETTINGS['sender_password'])
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Alert email sent: {subject}")
        return True
        
    except Exception as error:
        logger.error(f"Failed to send email: {error}")
        return False


def send_success_alert(pipeline_name, details):
    """
    Send a success alert email.
    """
    subject = f"SUCCESS: {pipeline_name} completed successfully"
    message = f"""
    The {pipeline_name} pipeline has completed successfully.
    
    Details:
    {details}
    
    No action is required.
    """
    return send_alert(subject, message, is_error=False)


def send_failure_alert(pipeline_name, error_message):
    """
    Send a failure alert email.
    """
    subject = f"FAILURE: {pipeline_name} failed"
    message = f"""
    The {pipeline_name} pipeline has failed.
    
    Error:
    {error_message}
    
    Please check the logs for more information.
    Action required!
    """
    return send_alert(subject, message, is_error=True)


def send_daily_summary(success_count, failure_count, details):
    """
    Send a daily summary email.
    """
    subject = f"Daily Pipeline Summary: {success_count} success, {failure_count} failures"
    message = f"""
    Daily Pipeline Summary
    
    Successes: {success_count}
    Failures: {failure_count}
    
    Details:
    {details}
    """
    return send_alert(subject, message, is_error=False)


# Test function
if __name__ == "__main__":
    print('-'*140)
    print("TESTING EMAIL ALERTS")
    print('-'*140)
    
    # Test success alert
    send_success_alert("Test Pipeline", "This is a test success message")
    
    # Test failure alert
    send_failure_alert("Test Pipeline", "This is a test error message")
    
    print('-'*140)
    print("Email test complete")
    print('-'*140)