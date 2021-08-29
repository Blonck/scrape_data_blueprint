from typing import Dict, Any, Literal
from pydantic import BaseModel


class TeamModel(BaseModel):
    """Data model for an NBA team"""
    name: str
    """Name of the team"""
    attributes: Dict[str, Any] = {}
    """Additional (optional) attributes of a team"""


class PlayerYearModel(BaseModel):
    """Data model for an NBA player for a specific year"""
    name: str
    """Name of the player"""
    team: str
    """Name of the team"""
    year: int
    """Year of the data"""
    attributes: Dict[str, Any] = {}
    """Additional (optional attributes)"""


class PlayerYearSeasonModel(PlayerYearModel):
    """Data model for an NBA player for a specific year and season"""
    season: Literal['postseason', 'regularseason']
