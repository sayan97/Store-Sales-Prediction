import os
import pandas as pd
from components.data_validation import prediction_data_validation

class data_validation_and_ingestion:
    def __init__(self,path,logger):
        self.logger=logger
        self.path=path
        self.raw_data=prediction_data_validation(self.path,self.logger)

    def validation(self):
        try:
            self.logger.log('Starting Data Validation')

            #validating file extension
            if self.raw_data.validate_file_extension()==False:
                self.logger.log('File extension validation failed')
                return False
            
            #validating number of columns
            elif self.raw_data.validate_columns_length()==False:
                self.logger.log('Number of columns validation failed')
                return False

            #validating column attributes
            elif self.raw_data.validate_column_attributes()==False:
                self.logger.log('Column attributes validation failed')
                return False
            
            #validating missing values
            elif self.raw_data.validate_missing_values()==False:
                self.logger.log('Missing values validation failed')
                return False
            
            else:
                self.logger.log('Data Validation Completed Successfully')
                return True
            
        except Exception as e:
            self.logger.log('ERROR : Unable to execute data validation : '+str(e))
            self.logger.close()
            raise e
        
    def ingestion(self):
        try:
            self.logger.log('Starting data ingestion')
            pred_data_path=os.path.join(self.path,'Final_Data')
            src_path=os.path.join(self.path,'Good_Raw')

            if not os.path.exists(pred_data_path):
                os.makedirs(pred_data_path)
    
            pred_data=pd.DataFrame()
    
            for file in os.listdir(src_path):
                csv=pd.read_csv(os.path.join(src_path,file))
                pred_data=pd.concat([pred_data,csv],axis=0)
            pred_data=pred_data.reset_index(drop=True)
            file_name='StoreSales_Prediction_Input.csv'
            pred_data.to_csv(os.path.join(pred_data_path,file_name),index=False)
            self.logger.log('Data ingestion completed. Final prediction data stored at '+pred_data_path)

        except Exception as e:
            self.logger.log('ERROR : Unable to perform data ingestion : '+str(e))
            self.logger.close()
            raise e
