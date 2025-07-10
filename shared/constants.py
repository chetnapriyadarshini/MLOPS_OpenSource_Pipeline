import os

ROOT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HOME_FOLDER = "/app"

DB_PATH = os.getenv("DB_PATH", "/app/db/")
CLEAN_DB = os.path.join(DB_PATH, "lead_scoring_data_cleaned.db")
INFERENCE_CLEAN_DB = os.path.join(DB_PATH, "lead_scoring_inference_data_cleaned.db")
EXPERIMENT_DB = os.path.join(DB_PATH, "lead_scoring_model_experimentation.db")
MLFLOW_PRODUCTION_DB = os.path.join(DB_PATH, "Lead_scoring_mlflow_production.db")
INITIAL_DATA_TABLE = 'loaded_data'
CITY_TIER_MAPPED_DATA_TABLE = 'city_tier_mapped'
CATEGORICAL_DATA_MAPPED_TABLE = 'categorical_variables_mapped'
INTERACTIONS_MAPPED_TABLE = "interactions_mapped"
MODEL_INPUT_TABLE="model_input"
SHARED_PATH = os.getenv("SHARED_PATH", "/app/shared")
INTERACTION_MAPPING = os.path.join(SHARED_PATH,'mapping','interaction_mapping.csv')
PREDICTION_PATH = os.path.join(ROOT_FOLDER,'predictions')

# mlflow experiment name
EXPERIMENT = 'Lead_scoring_mlflow_production'

TARGET_VAR = "app_complete_flag"
FEATURES_TABLE = "features"
TARGET_TABLE = "target"
UNSEEN_DATA_TABLE = "test_data"

MODELS_FOLDER = os.path.join(HOME_FOLDER, "models")
MODEL_NAME = "LightGBM"
STAGE = "Production"
EXPERIMENT = 'Lead_Scoring_Inference_Pipeline'
TARGET_VAR = "app_complete_flag"
PREDICTION = "predictions"
ACTUAL_VALUE_TABLE = "actual_target_value"
PREDICTION_DISTRIBUTION = "prediction_distribution.txt"
UNSEEN_DATA = "test_data"

ORIGINAL_FEATURES = ['total_leads_droppped', 'city_tier', 'referred_lead', 'app_complete_flag', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['city_tier', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = [
'app_complete_flag',
'total_leads_droppped',
'referred_lead',
'city_tier_1.0',
'city_tier_2.0',
'city_tier_3.0',
'first_platform_c_Level0',
'first_platform_c_Level1',
'first_platform_c_Level2',
'first_platform_c_Level3',
'first_platform_c_Level7',
'first_platform_c_Level8',
'first_platform_c_others',
'first_utm_medium_c_Level0' ,
'first_utm_medium_c_Level10',
'first_utm_medium_c_Level11', 
'first_utm_medium_c_Level13',
'first_utm_medium_c_Level15', 
'first_utm_medium_c_Level16',
'first_utm_medium_c_Level2',
'first_utm_medium_c_Level20',
'first_utm_medium_c_Level26', 
'first_utm_medium_c_Level3',
'first_utm_medium_c_Level30', 
'first_utm_medium_c_Level33',
'first_utm_medium_c_Level4',
'first_utm_medium_c_Level43',
'first_utm_medium_c_Level5',
'first_utm_medium_c_Level6',
'first_utm_medium_c_Level8',
'first_utm_medium_c_Level9',
'first_utm_medium_c_others',
'first_utm_source_c_Level0',
'first_utm_source_c_Level14', 
'first_utm_source_c_Level16',
'first_utm_source_c_Level2' ,
'first_utm_source_c_Level4',
'first_utm_source_c_Level5',
'first_utm_source_c_Level6',
'first_utm_source_c_Level7',
'first_utm_source_c_others']

INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead']
NOT_FEATURES = [
'created_date',
'assistance_interaction',
'career_interaction',
'payment_interaction',
'social_interaction',
'syllabus_interaction',
"city_mapped",
"interaction_type",
"1_on_1_industry_mentorship",                              
"call_us_button_clicked",                                 
"career_assistance",                                    
"career_coach",                                         
"career_impact",                                              
"careers",                                             
"chat_clicked",                                                
"companies",                                              
"download_button_clicked",                                     
"download_syllabus",                                   
"emi_partner_click",                                         
"emi_plans_clicked",                                         
"fee_component_click",                                         
"hiring_partners",                                       
"homepage_upgrad_support_number_clicked",                      
"industry_projects_case_studies",                     
"live_chat_button_clicked",                            
"payment_amount_toggle_mover",                                 
"placement_support",                               
"placement_support_banner_tab_clicked",                        
"program_structure",                      
"programme_curriculum",                                        
"programme_faculty",                                      
"request_callback_on_instant_customer_support_cta_clicked",    
"shorts_entry_click",  
"social_referral_click",                                       
"specialisation_tab_clicked",                                  
"specializations",                                
"specilization_click",                                         
"syllabus",                                       
"syllabus_expand",                                             
"syllabus_submodule_expand",                                   
"tab_career_assistance",                                 
"tab_job_opportunities",                                     
"tab_student_support",                                     
"view_programs_page",                                       
"whatsapp_chat_click"
]
