class KeePassMan:
    def __init__(self, db_dir, *args, **kwargs):
        from pykeepass import PyKeePass

        # PyKeePass Instance
        self.cred_retriever = PyKeePass(db_dir, *args, **kwargs)

    # Retrieves credentials
    def credential_manager(self, secret_id):
        return self.cred_retriever.find_entries(title=secret_id, first=True)