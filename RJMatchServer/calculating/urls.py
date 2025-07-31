from django.urls import path
from .views import calculate_match_degree, calculate_match_dimension_score


urlpatterns = [
    path('calculating/match_degree', calculate_match_degree),
    path('calculating/match_dimension_score', calculate_match_dimension_score)
]