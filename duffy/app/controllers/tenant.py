"""
This is the tenant controller.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from ...api_models import TenantCreateModel, TenantResult, TenantResultCollection
from ...database.model import Tenant
from ..database import req_db_async_session

router = APIRouter(prefix="/tenants")


# http get http://localhost:8080/api/v1/tenants
@router.get("", response_model=TenantResultCollection, tags=["tenants"])
async def get_all_tenants(db_async_session: AsyncSession = Depends(req_db_async_session)):
    """
    Return all tenants
    """
    query = select(Tenant)
    results = await db_async_session.execute(query)

    return {"action": "get", "tenants": results.scalars().all()}


# http get http://localhost:8080/api/v1/tenants/2
@router.get("/{id}", response_model=TenantResult, tags=["tenants"])
async def get_tenant(
    id: int,
    db_async_session: AsyncSession = Depends(req_db_async_session),
):
    """
    Return the tenant with the specified **ID**
    """
    tenant = (await db_async_session.execute(select(Tenant).filter_by(id=id))).scalar_one_or_none()

    if not tenant:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return {"action": "get", "tenant": tenant}


# http --json post http://localhost:8080/api/v1/tenants name="Unique name"
@router.post("", status_code=HTTP_201_CREATED, response_model=TenantResult, tags=["tenants"])
async def create_tenant(
    data: TenantCreateModel,
    db_async_session: AsyncSession = Depends(req_db_async_session),
):
    """
    Create a tenant with the specified **name**
    """
    tenant = Tenant(
        name=data.name, is_admin=data.is_admin, api_key=data.api_key, ssh_key=data.ssh_key
    )
    db_async_session.add(tenant)
    try:
        await db_async_session.commit()
    except IntegrityError as exc:
        raise HTTPException(HTTP_409_CONFLICT, str(exc))

    return {"action": "post", "tenant": tenant}


# http delete http://localhost:8080/api/v1/tenant/2
@router.delete("/{id}", response_model=TenantResult, tags=["tenants"])
async def delete_tenant(
    id: int,
    db_async_session: AsyncSession = Depends(req_db_async_session),
):
    """
    Delete the tenant with the specified **ID**
    """
    tenant = (await db_async_session.execute(select(Tenant).filter_by(id=id))).scalar_one_or_none()

    if not tenant:
        raise HTTPException(HTTP_404_NOT_FOUND)

    await db_async_session.delete(tenant)
    await db_async_session.commit()

    return {"action": "delete", "tenant": tenant}
