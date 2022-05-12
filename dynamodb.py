"""Module for DynamoDb related operations"""
from datetime import datetime
from ipaddress import ip_interface
import logging

from botocore.exceptions import ClientError, ParamValidationError
from boto3.dynamodb.conditions import Attr

from .aws_utils import AWSBaseSession

logger = logging.getLogger(__name__)


class DynamoDB:
    def __init__(self, rcc_account_session: AWSBaseSession, region: str, table: str):
        self.resource = rcc_account_session.dynamo_db_resource(region)
        self.table = table
        self.client = self.resource.meta.client
        self.table_obj = self.resource.Table(table)

    def scan_table(self, identifier: str) -> list:
        """Scan table for a given identifier.

        Args:
            identifier: Value to scan for
        Returns:
            list: Sorted array of items
        """
        items = []
        try:
            scan_kwargs = {
                "FilterExpression": (Attr("target_identifier").contains(identifier)),
                "ProjectionExpression": "col1, col2, col3",
            }
            resp = self.table_obj.scan(**scan_kwargs)
            items = resp.get("Items", [])
            while resp.get("LastEvaluateKey"):
                resp = self.table_obj.scan(**scan_kwargs, ExclusiveStartKey=resp["LastEvaluateKey"])
                items.extend(resp.get("Items"))
        except ClientError as exc:
            logger.exception(f"Error while scanning table {self.table}: {exc}")
            raise

        return sorted(items)

    def get_item(self, primary_key: dict) -> dict:
        """Get item by given key.

        Args:
            primary_key: Key to get the item
        Returns:
            dict: Record for given key if found
        """
        resp = {}
        try:
            resp = self.table_obj.get_item(Key=primary_key)
        except self.client.exceptions.ResourceNotFoundException:
            logger.exception(f"Record not found for key {primary_key}")
        except (ClientError, ParamValidationError) as exc:
            logger.exception(f"Unable to get item for {primary_key} from table {self.table}: {exc}")
        return resp

    def add_item(self, p_key: dict, data: dict) -> dict:
        """Add given item in table.

        Args:
            p_key: Key to add the item
            data: Data to be added
        Returns:
            dict: Add item response
        """
        resp = {}
        # Don't add items with None or empty value
        data = self.remove_none_attrs(data)
        try:
            resp = self.table_obj.put_item(Item={**p_key, **data})
        except (ClientError, ParamValidationError) as exc:
            logger.exception(f"Unable to add item for {p_key} in table {self.table}: {exc}")
            raise

        logger.info(f"Successfully added/updated item for {p_key} in table {self.table}: {data}")
        return resp

    def update_item(self, p_key: dict, data: dict) -> dict:
        """Update multiple attributes.

        Args:
            p_key: Key to update the item
            data: Data to be updated
        Returns:
            dict: Update item response
        """
        resp = {}
        # Don't add items with None or empty value
        data = self.remove_none_attrs(data)

        update_expr = ", ".join((f"#{key} = :{key}" for key in data.keys()))
        expr_attrnames = {f"#{key}": key for key in data.keys()}
        expr_attrvalues = {f":{key}": val for key, val in data.items()}
        try:
            resp = self.table_obj.update_item(
                Key=p_key,
                UpdateExpression=f"SET {update_expr}",
                ExpressionAttributeNames=expr_attrnames,
                ExpressionAttributeValues=expr_attrvalues,
            )
        except self.client.exceptions.ResourceNotFoundException:
            logger.exception(f"Record not found for key {p_key}")
            raise
        except (ClientError, ParamValidationError) as exc:
            logger.exception(f"Unable to update item for {p_key} in table {self.table}: {exc}")
            raise

        logger.info(f"Successfully updated data for {p_key} in table {self.table}")
        return resp
