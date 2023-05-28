import pandas as pd
import numpy as np
import pickle
import os

class preprocessing:
    
    def __init__(self,logger):
        self.logger=logger

    def edit_dataset(self,data):
        try:
            self.logger.log('Editing dataset')
            
            # imputing missing values
            data.loc[:, 'Item_Visibility'].replace([0], [data['Item_Visibility'].median()], inplace=True)

            # editing dataset
            data['Item_Fat_Content'].replace('low fat', 'Low Fat', inplace=True)
            data['Item_Fat_Content'].replace('LF', 'Low Fat', inplace=True)
            data['Item_Fat_Content'].replace('reg', 'Regular', inplace=True)

            data.loc[data['Item_Identifier']=="NC",'Item_Fat_Content']='Non Edible'

            self.logger.log('Dataset editing completed succesfully')
            return data

        except Exception as e:
            self.logger.log('ERROR : Unable to edit dataset : '+str(e))
            self.logger.close()
            raise e


    def encode_categorical_columns(self, data):
        try:
            self.logger.log('Encoding Categorical Columns')

            data['Outlet_Size']=data['Outlet_Size'].map({'Small':0, 'Medium': 1, 'High': 2})
    
            onehot_col=['Item_Identifier', 'Item_Fat_Content', 'Outlet_Type', 'Outlet_Location_Type']
            onehot_enc = pickle.load(open('Encoding/encoder.pickle', 'rb'))
            enc_array=onehot_enc.transform(data[onehot_col])
            enc_df=pd.DataFrame(enc_array)
            data=data.drop(columns=onehot_col)
            data=pd.concat([data, enc_df], axis=1)

            self.logger.log('Categorical columns encoded successfully')
            return data

        except Exception as e:
            self.logger.log('ERROR : Unable to encode categorical columns : '+str(e))
            self.logger.close()
            raise e
        
        
    def scale_numerical_columns(self,data):
        try:
            self.logger.log('Scaling Numerical Columns')
            num_cols=['Item_Visibility', 'Item_MRP', 'Outlet_Age']
            num_df=data[num_cols]
            cat_output_df=data.drop(columns=num_cols) #categorical and output columns
            scaler= pickle.load(open('Scaling\scaler.pickle', 'rb'))
            num_array=scaler.transform(num_df)
            num_df=pd.DataFrame(num_array, columns=num_df.columns)
            data=pd.concat([num_df, cat_output_df], axis=1)
            self.logger.log('Numerical columns scaled successfully')
            return data
        
        except Exception as e:
            self.logger.log('ERROR : Unable to scale numeric columns : '+str(e))
            self.logger.close()
            raise e
