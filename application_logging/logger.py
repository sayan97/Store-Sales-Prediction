import os
from datetime import datetime

class AppLogger:
    def __init__(self):
        timestamp = datetime.now().strftime('%d%m%Y_%H%M%S')
        file_name = f"log_{timestamp}.txt"
        file_path = "StoreSales_Logs"
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        log_file = os.path.join(file_path, file_name)
        self.file_obj = open(log_file, 'w')

    def log(self, message):
        self.now = datetime.now()
        self.date = str(self.now.date())
        self.time = str(self.now.strftime("%H:%M:%S"))

        self.file_obj.write("[{0} {1}]: {2}\n".format(self.date, self.time, message))
        self.file_obj.flush()

    def close(self):
        self.file_obj.close()






