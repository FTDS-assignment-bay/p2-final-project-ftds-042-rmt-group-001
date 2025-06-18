import os
import re
import json
import dill as pickle
import pandas as pd
import numpy as np
from huggingface_hub import hf_hub_download
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults

import logging

logging.basicConfig(level=logging.INFO)
logging.info("Ini info log")
logging.warning("Ini warning log")

class FixedValueModel:
    def __init__(self, value, name = None):
        self.value = value
        self.name = name

    def fit(self, X=None, y=None):
        pass

    def predict(self, start=None, end=None):
        if type(start) is int:
            return [self.value] * start
        elif isinstance(start, (list, tuple, np.ndarray, pd.Series)):
            return [self.value] * len(start)
        elif isinstance(start, pd.Timestamp) and isinstance(end, pd.Timestamp):
            return [self.value] * ((end - start).days + 1)
        return self.value

    def forecast(self, X=None):
        return self.predict(X)


class MovingAverageModel:
    def __init__(self, lag):
        """
        Initializes the MovingAverageModel with a given lag.
        """
        self.lag = lag
        self.train = None
        self.freq = None  # To store the inferred frequency

    def fit(self, time_series: pd.Series):
        """
        Fits the model to the provided time series data.
        """
        if not isinstance(time_series, pd.Series):
            raise ValueError("The time series must be a pandas Series.")
        
        # Store the training data
        self.train = time_series
        self.freq = pd.infer_freq(time_series.index)
        
        if self.freq is None:
            raise ValueError("The time series frequency could not be inferred.")
        
        print(f"Model fitted successfully with frequency: {self.freq}")
        logging.info(f"Model fitted successfully with frequency: {self.freq}")
        return self

    def predict(self, start=None, end=None) -> pd.Series:
        """
        Makes predictions for a given start and end date, or infers them from a series argument.
        """
        if self.train is None:
            raise ValueError("Model is not yet fitted. Call .fit() before predicting.")
        
        if isinstance(start,  pd.Series):
            # Infer start and end from the series
            end = start.index[-1]
            start = start.index[0]

        # Check if 'start' is within the range of 'train'
        if start <= self.train.index[-1]:
            # Prediction starts before or at the end of the training period
            result = self._predict_from_start_to_end(start, end, self.train)
        else:
            # 'start' is after the end of the training period
            # First, predict from end of train to start, then combine and predict forward
            first_prediction = self._predict_from_start_to_end(self.train.index[-1], start, self.train)
            extended_train = pd.concat([self.train, first_prediction])
            result = self._predict_from_start_to_end(extended_train.index[-1], end, extended_train)

        return result

    def _predict_from_start_to_end(self, start: pd.Timestamp, end: pd.Timestamp, base: pd.Series) -> pd.Series:
        """
        Helper function to predict when the 'start' date is before or within the training period.
        """
        # Initialize a list to store the predictions
        predictions = pd.Series(dtype=float)
        
        # For each date in the range from start to end
        for current_date in pd.date_range(start, end, freq=self.freq):
            # Get the relevant rolling window from the train data and the predictions
            relevant_window = base.loc[max(base.index.min(), current_date - pd.Timedelta(self.lag-1, unit=self.freq)):current_date]
            
            # Include previously predicted values (from self.predictions) in the rolling window
            if len(predictions) != 0:
                relevant_window = pd.concat([relevant_window, predictions])

            # Apply rolling mean
            rolling_mean = relevant_window.rolling(self.lag).mean().iloc[-1]  # Get the last rolling value
            
            # Store the predicted value in self.predictions for future use
            predictions = pd.concat([predictions, pd.Series([rolling_mean], index=[current_date])])

        return pd.Series(predictions, index=pd.date_range(start, end, freq=self.freq))

    def forecast(self, length: int) -> pd.Series:
        """
        Forecasts a future series based on the length.
        """
        if self.train is None:
            raise ValueError("Model is not yet fitted. Call .fit() before forecasting.")
        
        # Forecast starting from one period after the end of the training series
        start = self.train.index[-1] + self._get_offset(1)  # Move forward 1 period
        end = start + self._get_offset(length - 1)  # Forecast for 'length' periods
        
        return self.predict(start, end)

    def _get_offset(self, n: int) -> pd.DateOffset:
        """
        Returns a pd.DateOffset based on the inferred frequency and number of periods to move.
        """
        if self.freq == 'D':
            return pd.DateOffset(days=n)
        elif self.freq == 'H':
            return pd.DateOffset(hours=n)
        elif self.freq == 'T':
            return pd.DateOffset(minutes=n)
        elif self.freq == 'M':
            return pd.DateOffset(months=n)
        elif self.freq == 'Y':
            return pd.DateOffset(years=n)
        else:
            raise ValueError(f"Unsupported frequency: {self.freq}")

class ModelWrapper:
    komoditas_ids = None
    provinsi_ids = None

    def __init__(self):
        self.models = {}  # Dict[(komoditas, provinsi)] = model

    def register(self, komoditas, provinsi, model):
        self.models[(komoditas, provinsi)] = model

    def forecast(self, komoditas, provinsi, *args, **kwargs):
        model = self.models.get((komoditas, provinsi))
        if model is None:
            raise ValueError(f"No model registered for ({komoditas}, {provinsi})")
        if hasattr(model, 'forecast'):
            return model.forecast(*args, **kwargs)
        elif callable(model):
            return model(*args, **kwargs)
        else:
            raise TypeError("Model does not support forecasting.")

    def predict(self, komoditas, provinsi, *args, **kwargs):
        model = self.models.get((komoditas, provinsi))
        if model is None:
            raise ValueError(f"No model registered for ({komoditas}, {provinsi})")
        if hasattr(model, 'predict'):
            return model.predict(*args, **kwargs)
        elif callable(model):
            return model(*args, **kwargs)
        else:
            raise TypeError("Model does not support prediction.")

    def get_model_type(self, komoditas, provinsi):
        """
        Check the type of the model registered for the given komoditas and provinsi.
        """
        model = self.models.get((komoditas, provinsi))
        if model is None:
            raise ValueError(f"No model registered for ({komoditas}, {provinsi})")
        
        # Check if the model is ARIMA
        if isinstance(model, SARIMAX) or isinstance(model, SARIMAXResults) or 'SARIMA' in str(type(model)):
            return "SARIMA"
        
        # Check if the model is SARIMAX (SARIMA is a subclass of SARIMAX in statsmodels)
        elif isinstance(model, ARIMA) or isinstance(model, ARIMAResults) or 'ARIMA' in str(type(model)):
            return "ARIMA"
        
        # Check if the model is SARIMAX (SARIMA is a subclass of SARIMAX in statsmodels)
        elif 'HoltWinters' in str(type(model)):
            return "Holt-Winters"

        # Check for other model types
        elif isinstance(model, FixedValueModel):
            return model.name  # Add more checks for other model types
        
        else:
            return "Unknown Model Type: " + str(type(model))

    @classmethod
    def load_models_from_directory(cls, directory, komoditas_ids=None, provinsi_ids=None, hf_repo_id=None):
        """
        Load models from a directory or Hugging Face Hub if filename contains '.dummy.pkl'.
        If komoditas/provinsi lookup tables are not provided, attempt to load them from local JSON files.
        """
        if komoditas_ids is None:
            try:
                with open("komoditas_id.json", "r", encoding="utf-8") as f:
                    komoditas_ids = json.load(f)
                    cls.komoditas_ids = komoditas_ids
            except Exception as e:
                raise FileNotFoundError("Failed to load 'komoditas_id.json': " + str(e))

        if provinsi_ids is None:
            try:
                with open("provinsi_id.json", "r", encoding="utf-8") as f:
                    provinsi_ids = json.load(f)
                    cls.provinsi_ids = provinsi_ids
            except Exception as e:
                raise FileNotFoundError("Failed to load 'provinsi_id.json': " + str(e))

        wrapper = cls()
        pattern = re.compile(r'best_model_komoditas_(\d+)_provinsi_(\d+)\.pkl')

        for filename in os.listdir(directory):
            source = "local"
            adjusted_filename = filename

            if ".dummy.pkl" in filename:
                if not hf_repo_id:
                    print(f"Skipping {filename}: requires Hugging Face repo ID to download.")
                    logging.info(f"Skipping {filename}: requires Hugging Face repo ID to download.")
                    continue
                # Modify filename to match remote Hugging Face version
                adjusted_filename = filename.replace(".dummy", "")
                source = "huggingface"

            match = pattern.match(adjusted_filename)
            if not match:
                print(f"Skipping {filename}: does not match expected pattern.")
                logging.info(f"Skipping {filename}: does not match expected pattern.")
                continue

            komoditas_id, provinsi_id = match.groups()
            komoditas_name = komoditas_ids.get(komoditas_id)
            provinsi_name = provinsi_ids.get(provinsi_id)

            if not komoditas_name or not provinsi_name:
                print(f"Skipping {filename}: ID not found in lookup dictionaries.")
                logging.info(f"Skipping {filename}: ID not found in lookup dictionaries.")
                continue

            try:
                if source == "huggingface":
                    print(f"Fetching {adjusted_filename} from Hugging Face Hub...")
                    logging.info(f"Fetching {adjusted_filename} from Hugging Face Hub...")
                    model_path = hf_hub_download(repo_id=hf_repo_id, filename=adjusted_filename, cache_dir="/tmp/hf-cache")
                else:
                    model_path = os.path.join(directory, filename)

                with open(model_path, 'rb') as f:
                    model = pickle.load(f)

                if callable(model):
                    print(f"Loaded a function for {komoditas_name} - {provinsi_name}")
                    logging.info(f"Loaded a function for {komoditas_name} - {provinsi_name}")
                    if isinstance(model, str):
                        exec(model)
                        model = locals()['predictor']
                else:
                    print(f"Loaded model for {komoditas_name} - {provinsi_name}")
                    logging.info(f"Loaded model for {komoditas_name} - {provinsi_name}")

                wrapper.register(komoditas_name, provinsi_name, model)

            except Exception as e:
                print(f"Failed to load {filename}: {e}")
                logging.info(f"Failed to load {filename}: {e}")

        return wrapper
