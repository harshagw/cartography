import json
import logging
from typing import Dict
from typing import List

import boto3
import botocore.exceptions
import neo4j

from cartography.stats import get_stats_client
from cartography.util import aws_handle_regions
from cartography.util import dict_date_to_epoch
from cartography.util import merge_module_sync_metadata
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


@timeit
@aws_handle_regions
def get_meshes(boto3_session: boto3.session.Session, region: str) -> List[Dict]:
    client = boto3.client('appmesh', region_name=region)

    meshes = []

    try:
        meshes_paginator = client.get_paginator('list_meshes')

        vgateway_paginator = client.get_paginator('list_virtual_gateways')
        vnodes_paginator = client.get_paginator('list_virtual_nodes')
        vrouter_paginator = client.get_paginator('list_virtual_routers')
        vservices_paginator = client.get_paginator('list_virtual_services')

        gateway_routes_paginator = client.get_paginator('list_gateway_routes')
        routes_paginator = client.get_paginator('list_routes')

        for page in meshes_paginator.paginate():
            for mesh in page['meshes']:
                mesh["createdAt"] = dict_date_to_epoch(mesh, 'createdAt')

                virtual_gateway = []
                for service_page in vgateway_paginator.paginate(meshName=mesh['meshName']):
                    for service in service_page['virtualGateways']:
                        service["createdAt"] = dict_date_to_epoch(service, 'createdAt')

                        describe_response = client.describe_virtual_gateway(meshName=mesh['meshName'],
                                                                            virtualGatewayName=
                                                                            service['virtualGatewayName'])
                        service['spec'] = json.dumps(describe_response['virtualGateway']['spec'])
                        service['status'] = describe_response['virtualGateway']['status']

                        gateway_routes = []
                        for subservice_page in gateway_routes_paginator.paginate(meshName=mesh['meshName'],
                                                                                 virtualGatewayName=service
                                                                                 ['virtualGatewayName']):
                            for gateway_route in subservice_page['gatewayRoutes']:
                                gateway_route["createdAt"] = dict_date_to_epoch(gateway_route, 'createdAt')

                                gr_response = client.describe_gateway_route(
                                    gatewayRouteName=gateway_route['gatewayRouteName'],
                                    meshName=mesh['meshName'],
                                    virtualGatewayName=service['virtualGatewayName']
                                )

                                gateway_route['spec'] = json.dumps(gr_response['gatewayRoute']['spec'])
                                gateway_route['status'] = gr_response['gatewayRoute']['status']

                                gateway_routes.append(gateway_route)
                        service['gatewayRoutesList'] = gateway_routes

                        virtual_gateway.append(service)
                mesh['virtualGatewaysList'] = virtual_gateway

                virtual_nodes = []
                for service_page in vnodes_paginator.paginate(meshName=mesh['meshName']):
                    for service in service_page['virtualNodes']:
                        service["createdAt"] = dict_date_to_epoch(service, 'createdAt')
                        describe_response = client.describe_virtual_node(meshName=mesh['meshName'],
                                                                         virtualNodeName=service['virtualNodeName'])
                        service['spec'] = json.dumps(describe_response['virtualNode']['spec'])
                        service['status'] = describe_response['virtualNode']['status']
                        virtual_nodes.append(service)
                mesh['virtualNodesList'] = virtual_nodes

                virtual_routers = []
                for service_page in vrouter_paginator.paginate(meshName=mesh['meshName']):
                    for service in service_page['virtualRouters']:
                        service["createdAt"] = dict_date_to_epoch(service, 'createdAt')
                        describe_response = client.describe_virtual_router(meshName=mesh['meshName'],
                                                                           virtualRouterName=service[
                                                                               'virtualRouterName'])
                        service['spec'] = json.dumps(describe_response['virtualRouter']['spec'])
                        service['status'] = describe_response['virtualRouter']['status']
                        routes = []
                        for subservice_page in routes_paginator.paginate(meshName=mesh['meshName'],
                                                                         virtualRouterName=service
                                                                         ['virtualRouterName']):
                            for route in subservice_page['routes']:
                                route["createdAt"] = dict_date_to_epoch(route, 'createdAt')

                                r_response = client.describe_route(
                                    routeName=route['routeName'],
                                    meshName=mesh['meshName'],
                                    virtualRouterName=service['virtualRouterName']
                                )

                                route['spec'] = json.dumps(r_response['route']['spec'])
                                route['status'] = r_response['route']['status']

                                routes.append(route)
                        service['routesList'] = routes

                        virtual_routers.append(service)
                mesh['virtualRoutersList'] = virtual_routers

                virtual_services = []
                for service_page in vservices_paginator.paginate(meshName=mesh['meshName']):
                    for service in service_page['virtualServices']:
                        service["createdAt"] = dict_date_to_epoch(service, 'createdAt')
                        describe_response = client.describe_virtual_service(meshName=mesh['meshName'],
                                                                            virtualServiceName=service[
                                                                                'virtualServiceName'])
                        service['spec'] = json.dumps(describe_response['virtualService']['spec'])
                        service['status'] = describe_response['virtualService']['status']
                        virtual_services.append(service)
                mesh['virtualServicesList'] = virtual_services

                meshes.append(mesh)

    except botocore.exceptions.ClientError as e:
        logger.warning(
            "Could not run AppMesh - Client Error due to boto3 error %s: %s. Skipping.",
            e.response['Error']['Code'],
            e.response['Error']['Message'],
        )

    return meshes


@timeit
def load_appmesh(
        neo4j_session: neo4j.Session,
        data: List[Dict],
        region: str,
        current_aws_account_id: str,
        aws_update_tag: int,
) -> None:
    ingest_mesh = """
                 MERGE (d:AppMesh{id: $distribution.arn})
                 ON CREATE SET d.firstseen = timestamp()
                 SET d.arn = $distribution.arn,
                     d.name = $distribution.meshName,
                     d.region = $region,
                     d.owner = $distribution.meshOwner,
                     d.resource_owner = $distribution.resourceOwner,
                     d.version = $distribution.version,
                     d.created_at = $distribution.createdAt,
                     d.lastupdated = $aws_update_tag
                 WITH d
                 MATCH (owner:AWSAccount{id: $AWS_ACCOUNT_ID})
                 MERGE (owner)-[r:RESOURCE]->(d)
                 ON CREATE SET r.firstseen = timestamp()
                 SET r.lastupdated = $aws_update_tag
             """

    ingest_virtual_gateways = """
            UNWIND $list as l
            MERGE (d:MeshVirtualGateway{id: l.arn})
            ON CREATE SET d.firstseen = timestamp()
            SET d.arn = l.arn,
                d.name = l.virtualGatewayName,
                d.mesh_name = l.meshName,
                d.region = $region,
                d.mesh_owner = l.meshOwner,
                d.resource_owner = l.resourceOwner,
                d.version = l.version,
                d.spec = l.spec,
                        d.status = l.status,
                d.created_at = l.createdAt,
                d.lastupdated = $aws_update_tag
            WITH d
            MATCH (mesh:AppMesh{arn: $mesh_arn})
            MERGE (mesh)-[r:HAS_VIRTUAL_GATEWAY]->(d)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = $aws_update_tag
            """

    ingest_virtual_nodes = """
                UNWIND $list as l
                MERGE (d:MeshVirtualNode{id: l.arn})
                ON CREATE SET d.firstseen = timestamp()
                SET d.arn = l.arn,
                    d.name = l.virtualNodeName,
                    d.mesh_name = l.meshName,
                    d.region = $region,
                    d.mesh_owner = l.meshOwner,
                    d.resource_owner = l.resourceOwner,
                    d.version = l.version,
                    d.spec = l.spec,
                        d.status = l.status,
                    d.created_at = l.createdAt,
                    d.lastupdated = $aws_update_tag
                WITH d
                MATCH (mesh:AppMesh{arn: $mesh_arn})
                MERGE (mesh)-[r:HAS_VIRTUAL_NODE]->(d)
                ON CREATE SET r.firstseen = timestamp()
                SET r.lastupdated = $aws_update_tag
                """

    ingest_virtual_routers = """
                UNWIND $list as l
                MERGE (d:MeshVirtualRouter{id: l.arn})
                ON CREATE SET d.firstseen = timestamp()
                SET d.arn = l.arn,
                    d.name = l.virtualRouterName,
                    d.mesh_name = l.meshName,
                    d.region = $region,
                    d.mesh_owner = l.meshOwner,
                    d.resource_owner = l.resourceOwner,
                    d.version = l.version,
                    d.spec = l.spec,
                        d.status = l.status,
                    d.created_at = l.createdAt,
                    d.lastupdated = $aws_update_tag
                WITH d
                MATCH (mesh:AppMesh{arn: $mesh_arn})
                MERGE (mesh)-[r:HAS_VIRTUAL_ROUTER]->(d)
                ON CREATE SET r.firstseen = timestamp()
                SET r.lastupdated = $aws_update_tag
                """

    ingest_virtual_services = """
                UNWIND $list as l
                MERGE (d:MeshVirtualService{id: l.arn})
                ON CREATE SET d.firstseen = timestamp()
                SET d.arn = l.arn,
                    d.name = l.virtualServiceName,
                    d.mesh_name = l.meshName,
                    d.region = $region,
                    d.mesh_owner = l.meshOwner,
                    d.resource_owner = l.resourceOwner,
                    d.version = l.version,
                    d.spec = l.spec,
                        d.status = l.status,
                    d.created_at = l.createdAt,
                    d.lastupdated = $aws_update_tag
                WITH d
                MATCH (mesh:AppMesh{arn: $mesh_arn})
                MERGE (mesh)-[r:HAS_VIRTUAL_SERVICE]->(d)
                ON CREATE SET r.firstseen = timestamp()
                SET r.lastupdated = $aws_update_tag
                """

    ingest_gateway_routes = """
                    UNWIND $list as l
                    MERGE (d:MeshGatewayRoute{id: l.arn})
                    ON CREATE SET d.firstseen = timestamp()
                    SET d.arn = l.arn,
                        d.name = l.gatewayRouteName,
                        d.mesh_name = l.meshName,
                        d.region = $region,
                        d.mesh_owner = l.meshOwner,
                        d.resource_owner = l.resourceOwner,
                        d.version = l.version,
                        d.virutal_gateway_name = l.virtualGatewayName,
                        d.spec = l.spec,
                        d.status = l.status,
                        d.created_at = l.createdAt,
                        d.lastupdated = $aws_update_tag
                    WITH d
                    MATCH (vg:VirtualGateway{name: l.virtualGatewayName})
                    MERGE (vg)-[r:HAS_GATEWAY_ROUTE]->(d)
                    ON CREATE SET r.firstseen = timestamp()
                    SET r.lastupdated = $aws_update_tag
                    """

    ingest_routes = """
                        UNWIND $list as l
                        MERGE (d:MeshRoute{id: l.arn})
                        ON CREATE SET d.firstseen = timestamp()
                        SET d.arn = l.arn,
                            d.name = l.routeName,
                            d.mesh_name = l.meshName,
                            d.region = $region,
                            d.mesh_owner = l.meshOwner,
                            d.resource_owner = l.resourceOwner,
                            d.version = l.version,
                            d.virutal_router_name = l.virtualRouterName,
                            d.spec = l.spec,
                            d.status = l.status,
                            d.created_at = l.createdAt,
                            d.lastupdated = $aws_update_tag
                        WITH d
                        MATCH (vr:VirtualRouter{name: l.virtualRouterName})
                        MERGE (vr)-[r:HAS_ROUTE]->(d)
                        ON CREATE SET r.firstseen = timestamp()
                        SET r.lastupdated = $aws_update_tag
                        """

    for mesh in data:
        neo4j_session.run(
            ingest_mesh,
            distribution=mesh,
            region=region,
            AWS_ACCOUNT_ID=current_aws_account_id,
            aws_update_tag=aws_update_tag,
        )

        neo4j_session.run(
            ingest_virtual_services,
            list=mesh['virtualServicesList'],
            region=region,
            mesh_arn=mesh['arn'],
            aws_update_tag=aws_update_tag,
        )

        neo4j_session.run(
            ingest_virtual_gateways,
            list=mesh['virtualGatewaysList'],
            region=region,
            mesh_arn=mesh['arn'],
            aws_update_tag=aws_update_tag,
        )

        neo4j_session.run(
            ingest_virtual_routers,
            list=mesh['virtualRoutersList'],
            region=region,
            mesh_arn=mesh['arn'],
            aws_update_tag=aws_update_tag,
        )

        neo4j_session.run(
            ingest_virtual_nodes,
            list=mesh['virtualNodesList'],
            region=region,
            mesh_arn=mesh['arn'],
            aws_update_tag=aws_update_tag,
        )

        for virtual_gateway in mesh['virtualGatewaysList']:
            neo4j_session.run(
                ingest_gateway_routes,
                list=virtual_gateway['gatewayRoutesList'],
                region=region,
                aws_update_tag=aws_update_tag,
            )

        for virtual_router in mesh['virtualRoutersList']:
            neo4j_session.run(
                ingest_routes,
                list=virtual_router['routesList'],
                region=region,
                aws_update_tag=aws_update_tag,
            )


@timeit
def sync_appmesh(
        neo4j_session: neo4j.Session,
        boto3_session: boto3.session.Session,
        current_aws_account_id: str,
        aws_update_tag: int,
        region: str,
) -> None:
    meshes = get_meshes(boto3_session, region)
    load_appmesh(
        neo4j_session,
        meshes,
        region,
        current_aws_account_id,
        aws_update_tag,
    )


@timeit
def cleanup_appmesh(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('aws_import_elasticbeanstalk_cleanup.json', neo4j_session, common_job_parameters)


@timeit
def sync(
        neo4j_session: neo4j.Session,
        boto3_session: boto3.session.Session,
        regions: List[str],
        current_aws_account_id: str,
        update_tag: int,
        common_job_parameters: Dict,
) -> None:
    for region in regions:
        logger.info("Syncing AppMesh for region '%s' in account '%s'.", region, current_aws_account_id)
        sync_appmesh(neo4j_session, boto3_session, current_aws_account_id, update_tag, region)

    # cleanup_appmesh(neo4j_session, common_job_parameters)

    merge_module_sync_metadata(
        neo4j_session,
        group_type='AWSAccount',
        group_id=current_aws_account_id,
        synced_type='CloudFrontDistribution',
        update_tag=update_tag,
        stat_handler=stat_handler,
    )
