import pytest

@pytest.fixture
def mock_blob_client():
    class MockBlob:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def readall(self, *args, **kwargs):
            return None

        def readinto(self, *args, **kwargs):
            return None

    class MockBlobClient:
        def __init__(self, *args, **kwargs):
            pass

        def download_blob(self, *args, **kwargs):
            return MockBlob()

        def upload_blob(self, *args, **kwargs):
            pass

        def delete_blob(self, *args, **kwargs):
            pass

    yield MockBlobClient









