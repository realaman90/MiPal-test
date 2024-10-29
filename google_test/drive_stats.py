from typing import Dict, Optional
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from neo4j_test.user_store import UserStore
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DriveStats:
    """Class to get Google Drive statistics"""
    
    def __init__(self):
        """Initialize DriveStats with UserStore"""
        self.user_store = UserStore()

    def _get_drive_service(self, user_id: str):
        """Get Google Drive service for a user"""
        try:
            # Get credentials from Neo4j
            credentials = self.user_store.get_google_credentials(user_id)
            if not credentials:
                raise ValueError(f"No valid credentials found for user {user_id}")
            
            # Build and return the Drive service
            return build('drive', 'v3', credentials=credentials)
            
        except Exception as e:
            logger.error(f"Error getting Drive service: {e}")
            raise

    def get_file_count(self, user_id: str) -> Dict:
        """
        Get total number of files in Google Drive
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary containing file statistics
        """
        try:
            drive_service = self._get_drive_service(user_id)
            
            # Get all files (including trashed)
            all_files = drive_service.files().list(
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType, size, trashed)",
                q="'me' in owners"  # Only files owned by the user
            ).execute()
            
            files = all_files.get('files', [])
            
            # Initialize counters
            stats = {
                'total_files': 0,
                'active_files': 0,
                'trashed_files': 0,
                'total_size': 0,  # in bytes
                'by_type': {},
                'last_checked': datetime.now().isoformat()
            }
            
            # Count files
            for file in files:
                stats['total_files'] += 1
                
                if file.get('trashed', False):
                    stats['trashed_files'] += 1
                else:
                    stats['active_files'] += 1
                    
                    # Count by MIME type
                    mime_type = file.get('mimeType', 'unknown')
                    if mime_type not in stats['by_type']:
                        stats['by_type'][mime_type] = 0
                    stats['by_type'][mime_type] += 1
                    
                    # Add to total size if available
                    if 'size' in file:
                        stats['total_size'] += int(file['size'])
            
            # Update integration status in Neo4j
            self.user_store.update_integration_status(
                user_id=user_id,
                provider='google_drive',
                status_data={
                    'file_count': stats['active_files'],
                    'last_scanned': stats['last_checked']
                }
            )
            
            # Add human-readable size
            stats['total_size_human'] = self._format_size(stats['total_size'])
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting file count: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        
        finally:
            self.user_store.close()

    def _format_size(self, size_bytes: int) -> str:
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"

    def get_detailed_stats(self, user_id: str) -> Dict:
        """
        Get detailed statistics about Google Drive usage
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary containing detailed Drive statistics
        """
        try:
            drive_service = self._get_drive_service(user_id)
            
            # Get all files with detailed information
            files = drive_service.files().list(
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, shared, owners, lastModifyingUser, trashed)",
                q="'me' in owners"
            ).execute().get('files', [])
            
            stats = {
                'file_count': len(files),
                'active_files': sum(1 for f in files if not f.get('trashed', False)),
                'shared_files': sum(1 for f in files if f.get('shared', False)),
                'by_type': {},
                'by_month': {},
                'recent_activity': [],
                'storage_usage': 0,
                'last_checked': datetime.now().isoformat()
            }
            
            # Process each file
            for file in files:
                if file.get('trashed', False):
                    continue
                    
                # Count by type
                mime_type = file.get('mimeType', 'unknown')
                stats['by_type'][mime_type] = stats['by_type'].get(mime_type, 0) + 1
                
                # Count by creation month
                created_time = file.get('createdTime', '')[:7]  # YYYY-MM
                stats['by_month'][created_time] = stats['by_month'].get(created_time, 0) + 1
                
                # Add to storage usage
                if 'size' in file:
                    stats['storage_usage'] += int(file['size'])
                
                # Track recent modifications
                if 'modifiedTime' in file:
                    stats['recent_activity'].append({
                        'file_name': file['name'],
                        'modified_time': file['modifiedTime'],
                        'modified_by': file.get('lastModifyingUser', {}).get('displayName', 'Unknown')
                    })
            
            # Sort recent activity by date
            stats['recent_activity'].sort(key=lambda x: x['modified_time'], reverse=True)
            stats['recent_activity'] = stats['recent_activity'][:10]  # Keep only 10 most recent
            
            # Add human-readable storage usage
            stats['storage_usage_human'] = self._format_size(stats['storage_usage'])
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting detailed stats: {e}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        
        finally:
            self.user_store.close()

def main():
    """Test DriveStats functionality"""
    try:
        drive_stats = DriveStats()
        test_user_id = "aman"  # Use your test user ID
        
        # Get basic file count
        print("\nGetting basic file count...")
        count_stats = drive_stats.get_file_count(test_user_id)
        print("File Statistics:")
        print(f"Total Files: {count_stats['total_files']}")
        print(f"Active Files: {count_stats['active_files']}")
        print(f"Trashed Files: {count_stats['trashed_files']}")
        print(f"Total Size: {count_stats['total_size_human']}")
        print("\nFiles by Type:")
        for mime_type, count in count_stats['by_type'].items():
            print(f"- {mime_type}: {count}")
        
        # Get detailed statistics
        print("\nGetting detailed statistics...")
        detailed_stats = drive_stats.get_detailed_stats(test_user_id)
        print("\nDetailed Statistics:")
        print(f"Active Files: {detailed_stats['active_files']}")
        print(f"Shared Files: {detailed_stats['shared_files']}")
        print(f"Storage Usage: {detailed_stats['storage_usage_human']}")
        print("\nRecent Activity:")
        for activity in detailed_stats['recent_activity']:
            print(f"- {activity['file_name']} modified by {activity['modified_by']} at {activity['modified_time']}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 