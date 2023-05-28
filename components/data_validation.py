from os import listdir
import json
import os
import shutil
import pandas as pd


class prediction_data_validation:
    
    def __init__(self,path,logger):
        self.schema_path = 'schema_prediction.json'
        self.logger=logger
        self.batch_path=path
        self.create_batch_files_folder()

    def values_from_schema(self):
        try:
            with open(self.schema_path, 'r') as f:
                schema_dic = json.load(f)
                f.close()

            return schema_dic
        
        except Exception as e:
            self.logger.log('ERROR : Unable to read data validation schema values : '+str(e))
            self.logger.close()
            raise e
        
    def create_batch_files_folder(self):
        try:
            good_path=os.path.join(self.batch_path,'Good_Raw')
            if not os.path.exists(good_path):
                os.makedirs(good_path)

            bad_path=os.path.join(self.batch_path,'Bad_Raw')
            if not os.path.exists(bad_path):
                os.makedirs(bad_path)
            self.logger.log('Good_Raw and Bad_Raw folders created in batch files directory')
            
        except Exception as e:
            self.logger.log('ERROR : Unable to create batch files directory : '+str(e))
            self.logger.close()
            raise e


    def validate_file_extension(self):
        try:
            self.logger.log('Starting file extension validation')
            src_path=os.path.join(self.batch_path,'Raw_Data')
            good_path=os.path.join(self.batch_path,'Good_Raw')
            bad_path=os.path.join(self.batch_path,'Bad_Raw')
            
            for file in os.listdir(src_path):
                self.logger.log('Validating file extension for '+str(file))
                if str(file).lower().endswith('.csv')==False:
                    shutil.move(os.path.join(src_path,file),bad_path)
                    self.logger.log('File extension validation failed for {}, moved to Bad_Raw'.format(str(file)))
                else:
                    shutil.move(os.path.join(src_path,file),good_path)
                    self.logger.log('File extension validation passed for {}, moved to Good_Raw'.format(str(file)))
            
            if len(os.listdir(good_path))==0:
                self.logger.log('ERROR : None of the file extension dont match with .csv')
                return False
            else:
                self.logger.log('File extension validation completed : {} csv file accepted'.format(len(os.listdir(good_path))))
                return True
        except Exception as e:
            self.logger.log('ERROR : Unable to validate file extensions : '+str(e))
            self.logger.close()
            raise e

        
    def validate_columns_length(self):
        try:
            self.logger.log('Starting validation for number of columns')
            src_path=os.path.join(self.batch_path,'Good_Raw')
            dest_path=os.path.join(self.batch_path,'Bad_Raw')
            schema_dic=self.values_from_schema()
            for file in listdir(src_path):
                csv = pd.read_csv(os.path.join(src_path,file))
                self.logger.log('Validating number of columns in '+str(file))
                if csv.shape[1] == schema_dic['NumberofColumns']:
                    self.logger.log('Number of Columns Validated for '+str(file))
                else:
                    shutil.move(os.path.join(src_path,file),dest_path)
                    self.logger.log('Number of columns not validated for {}, sent to Bad_Raw'.format(str(file)))

            if len(os.listdir(src_path))==0:
                self.logger.log('ERROR : None of the file have required number of columns')
                return False
            else:
                self.logger.log('Number of columns validation completed : {} csv file accepted'.format(len(os.listdir(src_path))))
                return True
        
        except Exception as e:
            self.logger.log('ERROR : Unable to validate number of columns : '+str(e))
            self.logger.close()
            raise e


    def validate_column_attributes(self):
        try:
            self.logger.log('Starting validation for column attributes')
            src_path=os.path.join(self.batch_path,'Good_Raw')
            dest_path=os.path.join(self.batch_path,'Bad_Raw')
            schema_dic=self.values_from_schema()
            schema_cols=schema_dic['ColName']
            for file in listdir(src_path):
                self.logger.log('Validating column attributes in '+str(file))
                csv = pd.read_csv(os.path.join(src_path,file))
                count=0
                for col in csv.columns:
                    if col in schema_cols.keys():
                        self.logger.log('{0} column present in {1}'.format(col,file))
                        if csv[col].dtype==schema_cols[col]:
                            count=count+1
                            self.logger.log('Datatype validated for {0} column in {1}'.format(col,file))
                        else:
                            shutil.move(os.path.join(src_path,file),dest_path)
                            self.logger.log('Column datatypes not validated for {}, sent to Bad_Raw'.format(str(file)))
                    else:
                        shutil.move(os.path.join(src_path,file),dest_path)
                        self.logger.log('Column names not validated for {}, sent to Bad_Raw'.format(str(file)))

                if count==schema_dic["NumberofColumns"]:
                    self.logger.log('Column attributes validated for '+str(file))

            if len(os.listdir(src_path))==0:
                self.logger.log('ERROR : None of the file in Good_Raw have valid column attributes')
                return False
            else:
                self.logger.log('Column attribute validation completed : {} csv file accepted'.format(len(os.listdir(src_path))))
                return True
            
        except Exception as e:
            self.logger.log('ERROR : Unable to validate column attributes : '+str(e))
            self.logger.close()
            raise e
        
    
    def validate_missing_values(self):
        try:
            self.logger.log('Starting validation for missing values')
            src_path=os.path.join(self.batch_path,'Good_Raw')
            dest_path=os.path.join(self.batch_path,'Bad_Raw')
            for file in listdir(src_path):
                csv = pd.read_csv(os.path.join(src_path,file))
                self.logger.log('Validating missing values in '+str(file))
                if csv.isnull().values.any():
                    shutil.move(os.path.join(src_path,file),dest_path)
                    self.logger.log('Missing values present in {}, moved to Bad_Raw'.format(str(file)))
                else:
                    self.logger.log('No missing values present in '+str(file))
            
            if len(os.listdir(src_path))==0:
                self.logger.log('ERROR : All of the files have missing values, moved all to Bad_Raw')
                return False
            else:
                self.logger.log('Missing value validation completed : {} csv file accepted'.format(len(os.listdir(src_path))))
                return True
            
        except Exception as e:
            self.logger.log('ERROR : Unable to validate column attributes : '+str(e))
            self.logger.close()
            raise e






                










        

                                    

