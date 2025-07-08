from jose import jwt, JWTError
from datetime import timedelta, datetime
from fastapi import Header, HTTPException, status
from typing import Annotated
from my_secrets import *


def create_access_token(data: dict,
                        expires_delta: timedelta = timedelta(
                            minutes=ACCESS_TOKEN_EXPIRE_MINUTES)):
    """
    Generates a JWT access token with an expiration time.

    Parameters:
        data (dict): The payload to include in the token (e.g., user ID, role).
        expires_delta (timedelta, optional):
            Time until the token expires. Defaults to configured value.

    Returns:
        str: The encoded JWT token as a string.
    """
    to_encode = data.copy()
    expire = datetime.now() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(authorization: Annotated[str | None, Header()] = None):
    """
    Verifies the validity of a JWT token passed in the Authorization header.

    Parameters:
        authorization (str | None):
            Authorization header in the format "Bearer <token>".

    Returns:
        dict: The decoded token payload if verification succeeds.

    Raises:
        HTTPException: If the token is missing or invalid.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing token")

    token = authorization.split(" ")[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid token")
