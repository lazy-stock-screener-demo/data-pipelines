

class SaveParticularDocumentInStorageCommand(APICommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def test_upload(self):
        s3_client = boto3.client('s3')
        with open("./pipelines/fs_index-6955-000000695520000046.txt", "rb") as f:
            s3_client.upload_fileobj(f,
                                     'financial-index-from-filing-summary',
                                     'fs_index-6955-000000695520000046.txt',
                                     Callback=ProgressPercentage(
                                         './pipelines/fs_index-6955-000000695520000046.txt')
                                     )

    def execute(self):
        self.test_upload()
