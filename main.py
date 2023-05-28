from flask import Flask, render_template, send_file, request, redirect, url_for, flash
from flask_cors import CORS, cross_origin
import os
import shutil
from data_validation_and_ingestion import data_validation_and_ingestion
from application_logging.logger import AppLogger
from components.model_prediction import model_prediction


app=Flask(__name__)
CORS(app)
app.secret_key = "any random string"


@app.route("/", methods=['GET'])
@cross_origin()
def index():
    return render_template('index.html')

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html")


@app.route("/predict", methods=['POST'])
@cross_origin()
def predict():
    try:
        Item_Identifier=request.form['Item_Identifier']
        Item_Fat_Content=request.form['Item_Fat_Content']
        Item_Visibility=float(request.form['Item_Visibility'])
        Item_MRP=float(request.form['Item_MRP'])
        Outlet_Size=request.form['Outlet_Size']
        Outlet_Location_Type=request.form['Outlet_Location_Type']
        Outlet_Type=request.form['Outlet_Type']
        Outlet_Age=int(request.form['Outlet_Age'])


        data={
            "Item_Identifier" : Item_Identifier,
            "Item_Fat_Content" : Item_Fat_Content,
            "Item_Visibility" : Item_Visibility,
            "Item_MRP" : Item_MRP,
            "Outlet_Age" : Outlet_Age,
            "Outlet_Size" : Outlet_Size,
            "Outlet_Location_Type" : Outlet_Location_Type,
            "Outlet_Type" : Outlet_Type,
            }
        
        for item in data.keys():
            if data[item]==None:
                flash(f"Please Enter all the fields ", "danger")
                return redirect(url_for('index'))

        
        logger=AppLogger()
        logger.log('Received data for single prediction from user : '+str(data))
        
        pred=model_prediction(logger)
        output=pred.predict_for_single(data)

        flash(f"The predicted Item Outlet Sales : {output}", "success")
        return redirect(url_for('index'))
    
    except Exception as e:
        flash('Something went wrong', 'danger')
        return redirect(url_for('index'))
    

@app.route("/predict-dataset", methods=['POST'])
@cross_origin()
def predict_dataset():
    try:
        files = request.files.getlist('files')

        folderName = 'Prediction_Data_Batch_Files'
        if  os.path.isdir(folderName):
            shutil.rmtree(folderName)
        os.mkdir(folderName)

        raw_file_path=os.path.join(folderName,'Raw_Data')
        os.makedirs(raw_file_path)
        
        for file in files:
            file.save(os.path.join(raw_file_path, file.filename))

        logger=AppLogger()
        logger.log('Received {} files for bulk prediction'.format(len(os.listdir(folderName))))

        pred_val=data_validation_and_ingestion(folderName,logger)

        if pred_val.validation():
            pred_val.ingestion()
            pred=model_prediction(logger)
            output_folder=pred.predict_for_bulk(folderName)

            if os.path.exists(folderName):
                shutil.rmtree(folderName)

            logger.close()
            return send_file(output_folder, as_attachment=True)

        else:
            flash('Data Validation Failed ', 'danger')
            logger.close()
            return redirect(url_for('index'))
            
        
    except Exception as e:
        logger.log('ERROR : Unable to read the file from user')
        logger.close()
        flash('Something went wrong', 'danger')
        return redirect(url_for('index'))
    

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
