class ScraperError (Exception):
    def __init__(self, type, file="", message="Scraper has encountered an error"):
        self.type = type
        self.message = message
        self.file = file
        if(self.type):
            self.message = self.message + " of type: " + self.type
        if (self.file):
            self.message = self.message + " from file: " + self.file
        super().__init__(self.message)