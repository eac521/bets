# nba/nba_betting_model.py
import pickle
import pandas as pd
from .nba_base import NBABase

class NBABettingModel(NBABase):
    def __init__(self):
        super().__init__()
        self.models = {}
        self.scalers = {}
        self.load_all_models()
    
    def load_all_models(self):
        """Load all available models"""
        model_configs = {
            'threes': {
                'model_path': 'data/model/thrModV2.pkl',
                'scaler_path': 'data/model/scalValsThrees.pkl'
            },
            'points': {
                'model_path': 'data/model/pointsModel.pkl',
                'scaler_path': 'data/model/scalValsPoints.pkl'
            },
            'spread': {
                'model_path': 'data/model/spreadModel.pkl',
                'scaler_path': 'data/model/scalValsSpread.pkl'
            }
        }
        
        for model_name, paths in model_configs.items():
            try:
                self.models[model_name] = self.load_model(paths['model_path'])
                self.scalers[model_name] = self.load_scaler(paths['scaler_path'])
                print(f"Loaded {model_name} model successfully")
            except Exception as e:
                print(f"Could not load {model_name} model: {e}")
    
    def load_model(self, model_path):
        """Load a single model from pickle file"""
        with open(model_path, 'rb') as f:
            return pickle.load(f)
    
    def load_scaler(self, scaler_path):
        """Load scaling parameters"""
        with open(scaler_path, 'rb') as f:
            return pickle.load(f)
    
    def predict_threes(self, data):
        """Predict three-pointers made"""
        if 'threes' not in self.models:
            raise ValueError("Threes model not loaded")
        
        scaled_data = self.scale_data(data, 'threes')
        predictions = self.models['threes'].predict(scaled_data)
        return predictions
    
    def predict_points(self, data):
        """Predict total points scored"""
        if 'points' not in self.models:
            raise ValueError("Points model not loaded")
        
        scaled_data = self.scale_data(data, 'points')
        predictions = self.models['points'].predict(scaled_data)
        return predictions
    
    def predict_spread(self, data):
        """Predict team spread"""
        if 'spread' not in self.models:
            raise ValueError("Spread model not loaded")
        
        scaled_data = self.scale_data(data, 'spread')
        predictions = self.models['spread'].predict(scaled_data)
        return predictions
    
    def standardize_data(self, data, model_type):
        '''
        To standardize the data, using the saved scale
        '''
        scaler_params = self.scalers[model_type]
        scaled_data = data.copy()
        
        for col in scaler_params.keys():
            if col in scaled_data.columns:
                scaled_data[col] = (scaled_data[col] - scaler_params[col]['mean']) / scaler_params[col]['std']
        
        return scaled_data
    

    