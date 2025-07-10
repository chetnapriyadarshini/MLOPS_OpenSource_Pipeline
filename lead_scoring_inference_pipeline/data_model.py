from pydantic import BaseModel

class InputDataModel(BaseModel):
	total_leads_dropped: int
	city_mapped: object
	referred_lead: int
	first_platform_c: object
	first_utm_medium_c: object
	first_utm_source_c: object


class OutputDataModel(BaseModel):
    predicted_value: bool
    predicted_class: str
