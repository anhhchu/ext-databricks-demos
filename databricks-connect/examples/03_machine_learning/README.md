# Machine Learning with Databricks Connect

This example demonstrates how to build, train, and deploy machine learning models using Databricks Connect with MLflow integration.

## Overview

The ML pipeline includes:

1. **Data Preparation**: Feature engineering and preprocessing
2. **Model Training**: Training multiple ML models
3. **Model Evaluation**: Performance comparison and validation
4. **Model Deployment**: Model registration and serving
5. **Batch Inference**: Making predictions on new data

## Files

- `ml_pipeline.py` - Complete ML pipeline implementation
- `feature_engineering.py` - Feature engineering utilities
- `model_training.py` - Model training and evaluation
- `model_deployment.py` - Model registration and deployment
- `batch_inference.py` - Batch prediction examples

## Use Case: Employee Salary Prediction

The example predicts employee salaries based on:
- Department
- Location
- Years of experience
- Education level
- Performance metrics

## ML Pipeline Architecture

```
Raw Data → Feature Engineering → Model Training → Evaluation → Deployment
    ↓           ↓                    ↓              ↓          ↓
  CSV/JSON   Transformations    Multiple Models   MLflow    Model Registry
```

## Running the Examples

```bash
# Run the complete ML pipeline
python ml_pipeline.py

# Run feature engineering only
python feature_engineering.py

# Train and evaluate models
python model_training.py

# Deploy the best model
python model_deployment.py

# Run batch inference
python batch_inference.py
```

## Features Demonstrated

### Data Preparation
- Feature engineering with Spark
- Data splitting and validation
- Handling categorical variables
- Feature scaling and normalization

### Model Training
- Multiple algorithm comparison
- Hyperparameter tuning
- Cross-validation
- MLflow experiment tracking

### Model Evaluation
- Performance metrics calculation
- Model comparison and selection
- Validation and testing
- Feature importance analysis

### MLflow Integration
- Experiment tracking
- Model logging and versioning
- Parameter and metric logging
- Model registry management

## Models Trained

1. **Linear Regression** - Baseline model
2. **Random Forest** - Tree-based ensemble
3. **Gradient Boosting** - Advanced ensemble
4. **Neural Network** - Deep learning approach

## Expected Output

The pipeline will:
- Create MLflow experiments
- Log model metrics and parameters
- Register the best performing model
- Generate prediction results
- Create performance visualizations

## MLflow Integration

### Experiment Tracking
```python
import mlflow
import mlflow.sklearn

with mlflow.start_run():
    # Log parameters
    mlflow.log_param("model_type", "random_forest")
    
    # Log metrics
    mlflow.log_metric("rmse", rmse_score)
    
    # Log model
    mlflow.sklearn.log_model(model, "model")
```

### Model Registry
- Automatic model registration
- Version management
- Stage transitions (Staging → Production)
- Model metadata and lineage

## Best Practices

1. **Reproducibility**: Seed setting and environment management
2. **Scalability**: Distributed training with Spark ML
3. **Monitoring**: Model performance tracking
4. **Governance**: Model versioning and approval workflows
5. **Documentation**: Comprehensive model documentation 