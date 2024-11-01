from typing import Dict, List, Optional
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import openai
from neo4j_test.user_store import UserStore
from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText
import autogen
from bs4 import BeautifulSoup
import re

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailSearchService:
    def __init__(self):
        """Initialize connections and services"""
        # OpenAI setup
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize UserStore for Google credentials
        self.user_store = UserStore()
        
        # Initialize autogen agents
        self.config_list = [
            {
                'model': 'gpt-4o-mini',
                'api_key': self.openai_api_key
            }
        ]
        
        # Create assistant agent for query generation
        self.assistant = autogen.AssistantAgent(
            name="email_query_assistant",
            llm_config={
                "config_list": self.config_list,
                
            },
            system_message="""You are an expert at converting natural language queries about emails into Gmail search queries.
            Your task is to convert user requests into efficient Gmail search syntax.
            Consider date ranges, sender/recipient, labels, attachments, and keywords.
            Format your response as a valid Gmail search query."""
        )
        
        # Create user proxy agent
        self.user_proxy = autogen.UserProxyAgent(
            name="user_proxy",
            human_input_mode="NEVER",
            max_consecutive_auto_reply=1,
            is_termination_msg=lambda x: x.get("content", "").endswith("TERMINATE"),
            code_execution_config=False,
            llm_config={
                "config_list": self.config_list,
                "temperature": 0
            }
        )

    def close(self):
        """Close connections"""
        self.user_store.close()

    def _get_gmail_service(self, user_id: str):
        """Initialize Gmail service for a user"""
        try:
            credentials = self.user_store.get_google_credentials(user_id)
            if not credentials:
                raise ValueError(f"No valid Google credentials found for user {user_id}")
            
            return build('gmail', 'v1', credentials=credentials)
            
        except Exception as e:
            logger.error(f"Error getting Gmail service: {e}")
            raise

    def _generate_search_query(self, natural_query: str) -> str:
        """Generate Gmail search query using autogen agents"""
        try:
            # Create a more specific system message for Gmail query generation
            system_message = """You are an expert at converting natural language queries into Gmail search syntax.
            
            Use Gmail search operators like:
            - from: (sender)
            - to: (recipient)
            - subject: (subject line)
            - has:attachment
            - filename: (attachment name)
            - after: and before: (date)
            - in: (label)
            - is:starred/unread/important
            - larger: or smaller: (size)
            
            Examples:
            "Find emails from John about project deadlines last week"
            → "from:john after:2024/02/14 before:2024/02/21 (project deadline OR due date)"
            
            "Show me important emails with PDF attachments"
            → "has:attachment filename:pdf is:important"
            
            "Find unread messages about marketing campaign"
            → "is:unread (marketing campaign)"
            
            Return ONLY the search query, no explanations."""
            
            # Update assistant with better system message
            self.assistant = autogen.AssistantAgent(
                name="email_query_assistant",
                llm_config={
                    "config_list": [
                        {
                            'model': 'gpt-4o-mini',
                            'api_key': self.openai_api_key
                        }
                    ],
                    "temperature": 0.1  # Lower temperature for more precise queries
                },
                system_message=system_message
            )
            
            # Start conversation with clear instruction
            chat_response = self.user_proxy.initiate_chat(
                self.assistant,
                message=f"""Convert to Gmail search query: {natural_query}
                           Return ONLY the search query.
                           TERMINATE"""
            )
            
            # Get the last message from assistant
            messages = self.user_proxy.chat_messages.get(self.assistant.name, [])
            if not messages:
                return natural_query
                
            last_message = messages[-1]["content"]
            
            # Clean up the response
            query = last_message.strip()
            # Remove any explanations or extra text
            if "\n" in query:
                lines = [line.strip() for line in query.split("\n") if line.strip()]
                query = next((line for line in lines if not line.lower().startswith(("here", "this", "i", "the"))), lines[0])
            
            logger.info(f"Generated Gmail query: {query}")
            return query
            
        except Exception as e:
            logger.error(f"Error generating search query: {e}")
            return natural_query  # Fallback to original query

    def _extract_email_content(self, gmail_service, message_id: str) -> Dict:
        """Extract content from email message"""
        try:
            # Get full message
            message = gmail_service.users().messages().get(
                userId='me',
                id=message_id,
                format='full'
            ).execute()
            
            # Extract headers
            headers = message['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
            from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), 'Unknown')
            date = next((h['value'] for h in headers if h['name'].lower() == 'date'), 'Unknown')
            
            # Extract body
            body = self._get_email_body(message['payload'])
            
            # Clean body text
            clean_body = self._clean_email_text(body)
            
            return {
                'id': message_id,
                'thread_id': message['threadId'],
                'subject': subject,
                'from': from_email,
                'date': date,
                'body': clean_body,
                'labels': message['labelIds']
            }
            
        except Exception as e:
            logger.error(f"Error extracting email content: {e}")
            return {}

    def _get_email_body(self, payload: Dict) -> str:
        """Extract email body from payload"""
        if 'body' in payload and payload['body'].get('data'):
            return base64.urlsafe_b64decode(
                payload['body']['data'].encode('ASCII')
            ).decode('utf-8')
            
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] in ['text/plain', 'text/html']:
                    if 'data' in part['body']:
                        return base64.urlsafe_b64decode(
                            part['body']['data'].encode('ASCII')
                        ).decode('utf-8')
        
        return ""

    def _clean_email_text(self, text: str) -> str:
        """Clean and format email text"""
        try:
            # Convert HTML to text if needed
            if '<' in text and '>' in text:
                soup = BeautifulSoup(text, 'html.parser')
                text = soup.get_text()
            
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            # Remove common email signatures and footers
            signature_markers = [
                '-- \n',
                'Best regards,',
                'Regards,',
                'Thanks,',
                'Sent from my iPhone',
                'Get Outlook for'
            ]
            
            for marker in signature_markers:
                if marker in text:
                    text = text.split(marker)[0]
            
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error cleaning email text: {e}")
            return text

    def search_emails(self, user_id: str, natural_query: str, max_results: int = 5) -> List[Dict]:
        """Search emails using natural language query"""
        try:
            # Get Gmail service
            gmail_service = self._get_gmail_service(user_id)
            
            # Generate search query
            search_query = self._generate_search_query(natural_query)
            logger.info(f"Generated search query: {search_query}")
            
            # Search messages
            results = gmail_service.users().messages().list(
                userId='me',
                maxResults=max_results,
                q=search_query
            ).execute()
            
            messages = results.get('messages', [])
            emails = []
            
            for message in messages:
                email_data = self._extract_email_content(gmail_service, message['id'])
                if email_data:
                    emails.append(email_data)
            
            return emails
            
        except Exception as e:
            logger.error(f"Error searching emails: {e}")
            return []

    def get_email_summary(self, emails: List[Dict]) -> str:
        """Generate a summary of search results using GPT-4"""
        try:
            if not emails:
                return "No emails found."
            
            # Prepare email data for summarization
            email_texts = []
            for email in emails:
                email_texts.append(
                    f"Subject: {email['subject']}\n"
                    f"From: {email['from']}\n"
                    f"Date: {email['date']}\n"
                    f"Content: {email['body'][:500]}..."  # Limit content length
                )
            
            # Create prompt for GPT-4
            prompt = f"""Summarize these {len(emails)} emails:

            {'\n\n'.join(email_texts)}
            
            Provide a concise summary including:
            1. Main topics or themes
            2. Key points from important emails
            3. Any action items or important dates
            4. Notable senders or participants
            """
            
            # Get summary from GPT-4
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that summarizes email content."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return "Error generating email summary."

    def _download_attachment(self, gmail_service, user_id: str, message_id: str, attachment_id: str, filename: str, output_dir: str = "email_attachments") -> str:
        """Download an email attachment"""
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Get the attachment
            attachment = gmail_service.users().messages().attachments().get(
                userId=user_id,
                messageId=message_id,
                id=attachment_id
            ).execute()
            
            # Decode attachment data
            file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
            
            # Create safe filename
            safe_filename = "".join([c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')]).rstrip()
            filepath = os.path.join(output_dir, safe_filename)
            
            # Write to file
            with open(filepath, 'wb') as f:
                f.write(file_data)
                
            logger.info(f"Downloaded attachment: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return ""

    def get_email_attachments(self, user_id: str, query: str = None, max_results: int = 5) -> List[Dict]:
        """Get emails with attachments and download them"""
        try:
            gmail_service = self._get_gmail_service(user_id)
            
            # Build search query
            search_query = "has:attachment"
            if query:
                search_query += f" AND ({query})"
            
            # Search for messages with attachments
            results = gmail_service.users().messages().list(
                userId='me',
                maxResults=max_results,
                q=search_query
            ).execute()
            
            messages = results.get('messages', [])
            attachments = []
            
            for message in messages:
                # Get full message details
                msg = gmail_service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()
                
                # Extract headers
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
                from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), 'Unknown')
                date = next((h['value'] for h in headers if h['name'].lower() == 'date'), 'Unknown')
                
                # Process attachments
                if 'parts' in msg['payload']:
                    for part in msg['payload']['parts']:
                        if part.get('filename'):
                            attachment_data = {
                                'message_id': message['id'],
                                'subject': subject,
                                'from': from_email,
                                'date': date,
                                'filename': part['filename'],
                                'mime_type': part['mimeType'],
                                'size': part.get('body', {}).get('size', 0),
                                'attachment_id': part['body'].get('attachmentId')
                            }
                            
                            # Download attachment if it has an ID
                            if attachment_data['attachment_id']:
                                filepath = self._download_attachment(
                                    gmail_service,
                                    'me',
                                    message['id'],
                                    attachment_data['attachment_id'],
                                    attachment_data['filename']
                                )
                                attachment_data['local_path'] = filepath
                            
                            attachments.append(attachment_data)
            
            return attachments
            
        except Exception as e:
            logger.error(f"Error getting attachments: {e}")
            return []

    def download_specific_attachment(self, user_id: str, message_id: str, attachment_id: str, filename: str) -> str:
        """Download a specific attachment by message and attachment ID"""
        try:
            gmail_service = self._get_gmail_service(user_id)
            return self._download_attachment(
                gmail_service,
                'me',
                message_id,
                attachment_id,
                filename
            )
        except Exception as e:
            logger.error(f"Error downloading specific attachment: {e}")
            return ""

def main():
    """Test email search and attachment functionality"""
    try:
        email_service = EmailSearchService()
        test_user_id = "aman123"
        
        # Test getting attachments
        print("\nSearching for emails with attachments...")
        attachments = email_service.get_email_attachments(
            test_user_id,
            query="invoice OR receipt",  # Optional search terms
            max_results=5
        )
        
        if attachments:
            print(f"\nFound {len(attachments)} attachments:")
            for i, attachment in enumerate(attachments, 1):
                print(f"\n{i}. File: {attachment['filename']}")
                print(f"From: {attachment['from']}")
                print(f"Subject: {attachment['subject']}")
                print(f"Date: {attachment['date']}")
                print(f"Type: {attachment['mime_type']}")
                print(f"Size: {attachment['size']} bytes")
                if 'local_path' in attachment:
                    print(f"Downloaded to: {attachment['local_path']}")
                print("-" * 80)
        else:
            print("No attachments found")
        
        # Test regular search
        print("\nTesting email search...")
        test_queries = [
            {
                'text': "invoice attachments",
                'threshold': 0.3
            }
        ]
        
        for query_info in test_queries:
            print(f"\nSearching for: '{query_info['text']}'")
            results = email_service.search_emails(
                test_user_id,
                query_info['text'],
                max_results=5
            )
            
            if results:
                print(f"\nFound {len(results)} matching emails:")
                for i, email in enumerate(results, 1):
                    print(f"\n{i}. Subject: {email['subject']}")
                    print(f"From: {email['from']}")
                    print(f"Date: {email['date']}")
                    print(f"Preview: {email['body'][:200]}...")
                    print("-" * 80)
            else:
                print("No matching emails found")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        email_service.close()

if __name__ == "__main__":
    main() 