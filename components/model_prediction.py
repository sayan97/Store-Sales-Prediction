import os
import shutil
import pickle
import pandas as pd
import numpy as np
from components.data_preprocessing import preprocessing

class model_prediction:

    def __init__(self,logger):
        self.logger=logger

    def get_cluster_model(self,cluster):
        try:
            self.logger.log('Fetching Model for cluster : '+str(cluster))
            for file in os.listdir('Models'):
                modelName=file.split('.')[0]
                if modelName.endswith(str(cluster)):
                    model=pickle.load(open(f'Models/{file}', 'rb'))
            self.logger.log('Received model for cluster : '+str(cluster))
            return model
        
        except Exception as e:
            self.logger.log('ERROR : Cannot find model for cluster {0} : {1}'.format(str(cluster),str(e)))
            self.logger.close()
            raise e
        
    def predict_for_bulk(self,path):
        try:
            self.logger.log('Starting prediction for bulk data')
            preprocess = preprocessing(self.logger)
            src_path=os.path.join(path,'Final_Data')
            file_name='StoreSales_Prediction_Input.csv'

            data=pd.read_csv(os.path.join(src_path,file_name))

            # Data Preprocessing
            data=preprocess.edit_dataset(data)
            data=preprocess.encode_categorical_columns(data)
            data=preprocess.scale_numerical_columns(data)

            # Clustering
            self.logger.log('Starting Clustering of data')
            kmeans = pickle.load(open('Clustering/cluster.pickle', 'rb'))
            clusters = kmeans.predict(data.drop(columns=['Item_Code', 'Outlet_Code']))
            data['Cluster'] = clusters
            clusters = data['Cluster'].unique()
            self.logger.log('Clustering of data completed : Clusters found = '+str(clusters))
            
            final_output = pd.DataFrame()

            for i in clusters:
                cluster_data = data[data['Cluster'] == i]
                item_outlet = cluster_data[['Item_Code', 'Outlet_Code']]
                item_outlet = item_outlet.reset_index(drop=True)
                model=self.get_cluster_model(i)
                output = np.exp(model.predict(cluster_data.drop(['Cluster','Item_Code', 'Outlet_Code'],axis=1)))

                output = pd.DataFrame(output, columns=['Item_Outlet_Sales'])
                output = pd.concat([item_outlet, output], axis=1)

                final_output = pd.concat([final_output, output], axis=0, ignore_index=True)

            self.logger.log('Prediction for bulk data successful')
            output_folder='Final_Output_Folder/Prediction'
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)

            final_output.to_csv(os.path.join(output_folder,"Sales_Prediction_Output.csv"),header=True, index=None)

            bad_raw=os.path.join(path,'Bad_Raw')
            if len(os.listdir(bad_raw)) > 0:
                shutil.move(bad_raw,output_folder)

            shutil.make_archive(output_folder,'zip',output_folder)
            shutil.rmtree(output_folder)
            shutil.rmtree(path)

            self.logger.log('Prediction for bulk data completed successfully, output file created.')
            return output_folder + '.zip'
        
        except Exception as e:
            self.logger.log('ERROR : Unable to perform bulk data prediction : '+str(e))
            self.logger.close()
            raise e
        
    
    def predict_for_single(self,raw_data):
        try:
            self.logger.log('Starting prediction for single data')
            preprocess = preprocessing(self.logger)

            data = pd.DataFrame(raw_data, index=[0])

            # Data Preprocessing
            data=preprocess.edit_dataset(data)
            data=preprocess.encode_categorical_columns(data)
            data=preprocess.scale_numerical_columns(data)

            # Clustering
            self.logger.log('Starting Clustering of data')
            kmeans = pickle.load(open('Clustering/cluster.pickle', 'rb'))
            cluster = kmeans.predict(data)[0]
            self.logger.log('Data found in cluster '+str(cluster))

            model=self.get_cluster_model(cluster)
            output = np.round(np.exp(model.predict(data)[0]),4)

            self.logger.log('Prediction for single data completed successfully')
            self.logger.close()
            
            return output
        
        except Exception as e:
            self.logger.log('ERROR : Unable to perform single data prediction : '+str(e))
            self.logger.close()
            raise e






                

           
