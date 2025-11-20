class Database:
    def __init__(self):
        self.test = 'test' 
    
    def run(self, *args, **kwargs):
        print(f"Connecting to database -> host={kwargs.get('host')} username={kwargs.get('username')}")