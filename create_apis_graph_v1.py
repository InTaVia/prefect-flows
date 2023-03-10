import os
from pathlib import Path
import prefect
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun
from prefect import Flow, Parameter, flatten, task, unmapped
from copy import deepcopy
from datetime import datetime, timedelta
import requests
from rdflib import Graph, Literal, RDF, Namespace, URIRef
from rdflib.namespace import RDFS, XSD
from prefect.executors import LocalExecutor
from typing import Any, Tuple
from prefect.tasks.control_flow.filter import FilterTask
from prefect.tasks.shell import ShellTask

from prefect.engine.signals import LOOP, SUCCESS, SKIP, RETRY, FAIL

BASE_URL_API = 'http://localhost:5000/apis/api'
BASE_URI_SERIALIZATION = 'https://apis.acdh.oeaw.ac.at/'


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    return hours, minutes, seconds


def create_time_span_tripels(kind, event_node, obj, g):
    if kind == "start":
        if obj['start_date_written'] is not None:
            if len(obj[f"start_date_written"]) > 0:
                label_date = obj[f"start_date_written"]
                if obj['end_date_written'] is not None:
                    if len(obj["end_date_written"]) > 0:
                        label_date += f" - {obj['end_date_written']}"
                g.add((event_node, rdfs.label, Literal(label_date)))
    if len(obj[f'{kind}_date_written']) == 4:
        # check whether only a year has bin given for the start date and add according nodes
        if kind == "start":
            g.add((event_node, crm.P82a_begin_of_the_begin, (Literal(
                f"{obj[f'{kind}_date_written']}-01-01T00:00:00", datatype=XSD.dateTime))))
            g.add((event_node, crm.P81a_end_of_the_begin, (Literal(
                f"{obj[f'{kind}_date_written']}-12-31T23:59:59", datatype=XSD.dateTime))))
        elif kind == "end":
            g.add((event_node, crm.P82b_end_of_the_end, (Literal(
                f"{obj[f'{kind}_date_written']}-12-31T23:59:59", datatype=XSD.dateTime))))
            g.add((event_node, crm.P81b_begin_of_the_end, (Literal(
                f"{obj[f'{kind}_date_written']}-01-01T00:00:00", datatype=XSD.dateTime))))
    else:
        if kind == "start":
            g.add((event_node, crm.P82a_begin_of_the_begin, (Literal(
                f"{obj[f'{kind}_date']}T00:00:00", datatype=XSD.dateTime))))
        elif kind == "end":
            g.add((event_node, crm.P82b_end_of_the_end, (Literal(
                f"{obj[f'{kind}_date']}T23:59:59", datatype=XSD.dateTime))))
    return g


@task()
def render_personplace_relation(rel, g, base_uri):
    """renders personplace relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    place = None
    place_uri = URIRef(f"{idmapis}place/{rel['related_place']['id']}")
    if rel['relation_type']['id'] == 595:
        # define serialization for "person born in place relations"
        if (place_uri, None, None) not in g:
            pass
            # await render_place(rel['related_place']['id'], g)
        g.add(
            (URIRef(f"{idmapis}birthevent/{rel['related_person']['id']}"), crm.P7_took_place_at, place_uri))
    elif rel['relation_type']['id'] == 596:
        # define serialization for "person born in place relations"
        if (place_uri, None, None) not in g:
            pass
            # await render_place(rel['related_place']['id'], g)
        g.add(
            (URIRef(f"{idmapis}deathevent/{rel['related_person']['id']}"), crm.P7_took_place_at, place_uri))
    else:
        event_uri = URIRef(f"{idmapis}event/personplace/{rel['id']}")
        if (event_uri, None, None) not in g:
            g = render_event(rel, 'personplace', event_uri, g)
        if (place_uri, None, None) not in g and rel["related_place"]["id"] not in glob_list_entities["places"]:
            place = rel['related_place']['id']
            glob_list_entities["places"].append(rel["related_place"]["id"])
        g.add((event_uri, crm.P7_took_place_at, place_uri))
    return place


@task()
def render_personperson_relation(rel, g):
    """renders personperson relation as RDF graph

    Args:
        pers_uri (_type_): _description_
        rel (_type_): _description_
        g (_type_): _description_http://localhost:8000/apis/api/entities/person/
    """
    # prepare nodes
    logger = prefect.context.get("logger")
    if isinstance(rel, list):
        if len(rel) == 0:
            return SKIP(message="No person-person relations found skipping")
    person = None
    pers_uri = URIRef(f"{idmapis}personproxy/{rel['related_personA']['id']}")
    n_rel_type = URIRef(f"{idmapis}personrelation/{rel['id']}")
    n_relationtype = URIRef(f"{idmrelations}{rel['relation_type']['id']}")
    g.add((pers_uri, bioc.has_person_relation, n_rel_type))
    g.add((n_rel_type, RDF.type, n_relationtype))
    g.add((n_rel_type, RDFS.label, Literal(
        f"{rel['relation_type']['label']}")))
    g.add((URIRef(
        f"{idmapis}personproxy/{rel['related_personB']['id']}"), bioc.inheres_in, n_rel_type))
    if rel['related_personB']['id'] not in glob_list_entities["persons"]:
        glob_list_entities["persons"].append(rel["related_personB"]["id"])
        person = rel['related_personB']['id']
    # g.add(n_rel_type, bioc.bearer_of, (URIRef(
    #    f"{idmapis}personproxy/{rel['related_personB']['id']}")))
    # TODO: add hiarachy of person relations
    if rel['relation_type'] is not None:
        if rel['relation_type']['label'] != "undefined":
            if rel['relation_type']['parent_id'] is not None:
                g.add((n_relationtype, RDFS.subClassOf, URIRef(
                    f"{idmrelations}{rel['relation_type']['parent_id']}")))
                g.add((URIRef(f"{idmrelations}{rel['relation_type']['parent_id']}"), RDFS.subClassOf, URIRef(
                    bioc.Person_Relationship_Role)))
    else:
        g.add((n_relationtype, RDFS.subClassOf, bioc.Person_Relationship_Role))
    logger.info(
        f" personpersonrelation serialized for: {rel['related_personA']['id']}")
    # return g
    # group which is part of this relation
    return person


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def render_personrole(rel, g, base_uri):
    """renders personrole as RDF graph

    Args:
        pers_uri (_type_): _description_
        role (_type_): _description_
        g (_type_): _description_
    """
    # prepare nodes
    logger = prefect.context.get("logger")
    loop_payload = prefect.context.get("task_loop_result", None)
    if loop_payload is not None:
        role = loop_payload.get("role")
        second_entity = loop_payload.get("second_entity")
    elif (URIRef(f"{idmapis}personrole/{rel['relation_type']['id']}"), None, None) not in g:
        for k in rel.keys():
            if k.startswith("related_"):
                if k.split('_')[1] != "person":
                    role = rel["relation_type"]
                    second_entity = k.split('_')[1]
    else:
        raise SKIP(message="personrole already in graph")
    label = "label"
    if "label" not in role:
        label = "name"
    parent = "parent_id"
    if "parent_id" not in role:
        parent = "parent_class"
    n_role = URIRef(f"{idmapis}personrole/{role['id']}")
    g.add((n_role, RDFS.label, Literal(f"{role[label]}", lang="de")))
    if role[parent] is not None:
        if 'parent_id' in role:
            p_id = role['parent_id']
        else:
            p_id = role['parent_class']['id']
        if (URIRef(f"{idmapis}personrole/{p_id}"), None, None) not in g:
            res = requests.get(
                base_uri + f"/apis/api/vocabularies/person{second_entity}relation/{role['parent_id']}")
            if res.status_code != 200:
                logger.warn(
                    f"Error getting relation type: {res.status_code} / {res.text}")
            role2 = res.json()
            raise LOOP(
                result={"role": role2, "second_entity": second_entity, "g": g})
    else:
        g.add((n_role, RDF.type, bioc.Actor_Role))
    return g


@task
def render_personinstitution_relation(rel: dict, g: Graph) -> list:
    logger = prefect.context.get("logger")
    pers_uri = URIRef(f"{idmapis}personproxy/{rel['related_person']['id']}")
    inst = None
    # connect personproxy and institutions with grouprelationship
    n_rel_type = URIRef(f"{idmapis}grouprelation/{rel['id']}")
    g.add((pers_uri, bioc.has_group_relation, n_rel_type))
    # Person has a specific group relation
    g.add((n_rel_type, RDF.type, URIRef(
        f"{idmapis}grouprole/{rel['relation_type']['id']}")))
    # define type of grouprelation
    if rel['relation_type']['parent_id'] is not None:
        # if the relationtype has a superclass, it is added here
        g.add((URIRef(f"{idmapis}grouprole/{rel['relation_type']['id']}"),
              rdfs.subClassOf, URIRef(f"{idmapis}grouprole/{rel['relation_type']['parent_id']}")))
    g.add((n_rel_type, rdfs.label, Literal(rel['relation_type']['label'])))
    # add label to relationtype
    g.add((n_rel_type, bioc.inheres_in, URIRef(
        f"{idmapis}groupproxy/{rel['related_institution']['id']}")))
    # g.add((URIRef(
    #    f"{idmapis}groupproxy/{rel['related_institution']['id']}"), bioc.bearer_of, n_rel_type))
    # group which is part of this relation
    g.add((URIRef(f"{idmapis}career/{rel['id']}"), RDF.type, idmcore.Career))
    # add career event of type idmcore:career
    g.add((idmcore.Career, rdfs.subClassOf, crm.E5_Event,))
    g.add((URIRef(f"{idmapis}career/{rel['id']}"), rdfs.label, Literal(
        f"{rel['related_person']['label']} {rel['relation_type']['label']} {rel['related_institution']['label']}")))
    # label for career event
    g.add((URIRef(f"{idmapis}career/{rel['id']}"), bioc.had_participant_in_role, URIRef(
        f"{idmapis}personrole/{rel['id']}/{rel['related_person']['id']}")))
    # role of participating person in the career event
    g.add((URIRef(f"{idmapis}personproxy/{rel['related_person']['id']}"),
           bioc.bearer_of, URIRef(f"{idmapis}personrole/{rel['id']}/{rel['related_person']['id']}")))
    g.add((URIRef(f"{idmapis}personrole/{rel['id']}/{rel['related_person']['id']}"),
          RDF.type, URIRef(f"{idmapis}personrole/{rel['relation_type']['id']}")))
    if rel['relation_type']['parent_id'] is not None:
        g.add((URIRef(f"{idmapis}personrole/{rel['relation_type']['id']}"), RDF.type, URIRef(
            f"{idmapis}personrole/{rel['relation_type']['parent_id']}")))
    # person which inheres this role
    g.add((URIRef(f"{idmapis}career/{rel['id']}"), bioc.had_participant_in_role, URIRef(
        f"{idmapis}grouprole/{rel['id']}/{rel['related_institution']['id']}")))
    # role of institution/ group in the career event
    g.add((URIRef(
        f"{idmapis}grouprole/{rel['id']}/{rel['related_institution']['id']}"), RDF.type, bioc.Group_Relationship_Role))
    g.add((URIRef(f"{idmapis}grouprole/{rel['id']}/{rel['related_institution']['id']}"),
          bioc.inheres_in, URIRef(f"{idmapis}groupproxy/{rel['related_institution']['id']}")))
    # role which inheres the institution/ group
    if (URIRef(f"{idmapis}groupproxy/{rel['related_institution']['id']}"), None, None) not in g:
        if rel['related_institution']['id'] not in glob_list_entities["institutions"]:
            inst = rel['related_institution']['id']
            glob_list_entities["institutions"].append(
                rel['related_institution']['id'])
    g.add((URIRef(f"{idmapis}career/{rel['id']}"), URIRef(
        f"{crm}P4_has_time-span"), URIRef(f"{idmapis}career/timespan/{rel['id']}")))
    for rel_plcs in g.objects(URIRef(f"{idmapis}groupproxy/{rel['related_institution']['id']}"), crm.P74_has_current_or_former_residence):
        g.add(
            (URIRef(f"{idmapis}career/{rel['id']}"), crm.P7_took_place_at, rel_plcs))
    logger.info(
        f" personinstitutionrelation serialized for: {rel['related_person']['id']}")
    if rel['start_date'] is not None:
        g = create_time_span_tripels('start', URIRef(
            f"{idmapis}career/timespan/{rel['id']}"), rel, g)
    if rel['end_date'] is not None:
        g = create_time_span_tripels('end', URIRef(
            f"{idmapis}career/timespan/{rel['id']}"), rel, g)
    """     if (rel['start_date'] is not None) and (rel['end_date'] is not None):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            rel['start_date_written'])+' - ' + rel['end_date_written']))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82a_begin_of_the_begin, (Literal(
            rel['start_date']+'T00:00:00', datatype=XSD.dateTime))))
        g.add((URIRef(
            f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(rel['start_date_written'])))
    elif ((rel['start_date'] is not None) and (rel['end_date'] is not None)):
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), crm.P82b_end_of_the_end, (Literal(
            rel['end_date']+'T23:59:59', datatype=XSD.dateTime))))
        g.add((URIRef(f"{idmapis}career/timespan/{rel['id']}"), rdfs.label, Literal(
            'time-span end:' + rel['end_date_written']))) """
    return inst


@task()
def render_person(person, g, base_uri):
    """renders person object as RDF graph

    Args:
        person (_type_): _description_
        g (_type_): _description_
    """
    pers_uri = URIRef(f"{idmapis}personproxy/{person['id']}")
    if (pers_uri, None, None) in g:
        return g
    g.add((pers_uri, RDF.type, crm.E21_Person))
    g.add((pers_uri, RDF.type, idmcore.Person_Proxy))
    g.add((pers_uri, RDFS.label, Literal(
        f"{person['first_name']} {person['name']}")))
    # define that individual in APIS named graph and APIS entity are the same
    g.add((pers_uri, owl.sameAs, URIRef(
        f"{base_uri}/entity/{person['id']}")))
    # add sameAs
    # add appellations
    node_main_appellation = URIRef(
        f"{idmapis}appellation/label/{person['id']}")
    g.add((node_main_appellation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    g.add((node_main_appellation, RDFS.label, Literal(
        f"{person['name'] if person['name'] is not None else '-'}, {person['first_name'] if person['first_name'] is not None else '-'}")))
    g.add((pers_uri, crm.P1_is_identified_by, node_main_appellation))
    if person['first_name'] is not None:
        node_first_name_appellation = URIRef(
            f"{idmapis}appellation/first_name/{person['id']}")
        g.add((node_first_name_appellation, RDF.type,
              crm.E33_E41_Linguistic_Appellation))
        g.add((node_first_name_appellation, RDFS.label,
              Literal(person['first_name'])))
        g.add((node_main_appellation, crm.P148_has_component,
              node_first_name_appellation))
    if person['name'] is not None:
        node_last_name_appellation = URIRef(
            f"{idmapis}appellation/last_name/{person['id']}")
        g.add((node_last_name_appellation, RDF.type,
              crm.E33_E41_Linguistic_Appellation))
        g.add((node_last_name_appellation,
              RDFS.label, Literal(person['name'])))
        g.add((node_main_appellation, crm.P148_has_component,
              node_last_name_appellation))
    if person['start_date'] is not None:
        node_birth_event = URIRef(f"{idmapis}birthevent/{person['id']}")
        node_role = URIRef(f"{idmapis}born_person/{person['id']}")
        node_role_class = URIRef(f"{idmrole}born_person")
        node_time_span = URIRef(f"{idmapis}birth/timespan/{person['id']}")
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_birth_event, bioc.had_participant_in_role, node_role))
        g.add((node_birth_event, RDF.type, crm.E67_Birth))
        g.add((node_birth_event, RDFS.label, Literal(
            f"Birth of {person['first_name']} {person['name']}")))
        g.add((node_birth_event, URIRef(crm + 'P4_has_time-span'), node_time_span))
        g.add((node_birth_event, crm.P98_brought_into_life, pers_uri))
        g = create_time_span_tripels('start', node_time_span, person, g)
    if person['end_date'] is not None:
        node_death_event = URIRef(f"{idmapis}deathevent/{person['id']}")
        node_role = URIRef(f"{idmapis}deceased_person/{person['id']}")
        node_role_class = URIRef(f"{idmrole}deceased_person")
        node_time_span = URIRef(f"{idmapis}death/timespan/{person['id']}")
        g.add((node_role, bioc.inheres_in, pers_uri))
        g.add((node_role, RDF.type, node_role_class))
        g.add((node_role_class, rdfs.subClassOf, bioc.Event_Role))
        g.add((node_death_event, bioc.had_participant_in_role, node_role))
        g.add((node_death_event, RDF.type, crm.E69_Death))
        g.add((node_death_event, RDFS.label, Literal(
            f"Death of {person['first_name']} {person['name']}")))
        g.add((node_death_event, URIRef(crm + 'P4_has_time-span'), node_time_span))
        g.add((node_death_event, crm.P100_was_death_of, pers_uri))
        g = create_time_span_tripels('end', node_time_span, person, g)
    for prof in person['profession']:
        prof_node = URIRef(f"{idmapis}occupation/{prof['id']}")
        g.add((pers_uri, bioc.has_occupation, prof_node))
        g.add((prof_node, rdfs.label, Literal(prof['label'])))
        if prof['parent_id'] is not None:
            parent_prof_node = URIRef(
                f"{idmapis}occupation/{prof['parent_id']}")
            g.add((prof_node, rdfs.subClassOf, parent_prof_node))
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
        else:
            g.add((prof_node, rdfs.subClassOf, bioc.Occupation))
    if person["gender"] is not None:
        g.add((pers_uri, bioc.has_gender, bioc[person["gender"].capitalize()]))
    for uri in person['sameAs']:
        g.add((pers_uri, owl.sameAs, URIRef(uri)))
    if "text" in person:
        if len(person['text']) > 1:
            g.add((pers_uri), idmcore.bio_link,
                  URIRef(person['text'][0]['url']))
            g.add((pers_uri), idmcore.short_bio_link,
                  URIRef(person['text'][1]['url']))
    # add occupations

#    person_rel = await get_person_relations(person['id'], kinds=['personinstitution', 'personperson', 'personplace'])
    # tasks = []
    # for rel_type, rel_list in person_rel.items():
    #     if rel_type == 'personinstitution':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personinstitution_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personperson':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personperson_relation(pers_uri, rel, g)))
    #     elif rel_type == 'personplace':
    #         for rel in rel_list:
    #             tasks.append(asyncio.create_task(
    #                 render_personplace_relation(pers_uri, rel, g)))
 #   await asyncio.gather(*tasks)
    return g


@task()
def render_organizationplace_relation(rel, g):
    place = None
    node_org = URIRef(
        f"{idmapis}groupproxy/{rel['related_institution']['id']}")
    if (URIRef(f"{idmapis}place/{rel['related_place']['id']}"), None, None) not in g and rel['related_place']['id'] not in glob_list_entities["places"]:
        place = rel['related_place']['id']
        glob_list_entities["places"].append(place)
    g.add(
        (node_org, crm.P74_has_current_or_former_residence, URIRef(f"{idmapis}place/{rel['related_place']['id']}")))
    return place


@task()
def render_organization(organization, g, base_uri):
    """renders organization object as RDF graph

    Args:
        organization (_type_): _description_
        g (_type_): _description_
    """
#    res = await get_entity(organization, "institution")
#    res_relations = await get_organization_relations(organization, ["institutionplace"])
    # setup basic nodes
    node_org = URIRef(f"{idmapis}groupproxy/{organization['id']}")
    appelation_org = URIRef(f"{idmapis}groupappellation/{organization['id']}")
    # connect Group Proxy and person in named graphbgn:BioDes
    g.add((node_org, RDF.type, crm.E74_Group))
    g.add((node_org, RDF.type, idmcore.Group))
    # defines group class
    g.add((node_org, owl.sameAs, URIRef(
        f"{base_uri}/entity/{organization['id']}")))
    for uri in organization['sameAs']:
        g.add((node_org, owl.sameAs, URIRef(uri)))
    # defines group as the same group in the APIS dataset
    g.add((node_org, crm.P1_is_identified_by, appelation_org))
    g.add((appelation_org, rdfs.label, Literal(organization['name'])))
    g.add((appelation_org, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # add group appellation and define it as linguistic appellation
    if organization["start_date_written"] is not None:
        if len(organization['start_date_written']) >= 4:
            start_date_node = URIRef(
                f"{idmapis}groupstart/{organization['id']}")
            start_date_time_span = URIRef(
                f"{idmapis}groupstart/timespan/{organization['id']}")
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((start_date_node, RDF.type, crm.E63_Beginning_of_Existence))
            g.add((start_date_node, crm.P92_brought_into_existence, node_org))
            g.add((start_date_node, URIRef(
                crm + "P4_has_time-span"), start_date_time_span))
            g = create_time_span_tripels(
                'start', start_date_time_span, organization, g)
            # if len(res['start_date_written']) == 4 and res['start_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((start_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((start_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['start_date']}T23:59:59", datatype=XSD.dateTime))))
    if organization["end_date_written"] is not None:
        if len(organization['end_date_written']) >= 4:
            end_date_node = URIRef(f"{idmapis}groupend/{organization['id']}")
            end_date_time_span = URIRef(
                f"{idmapis}groupend/timespan/{organization['id']}")
            # print(row['institution_name'], ':', row['institution_start_date'], row['institution_end_date'], row['institution_start_date_written'], row['institution_end_date_written'])
            g.add((end_date_node, RDF.type, crm.E64_End_of_Existence))
            g.add((end_date_node, crm.P93_took_out_of_existence, node_org))
            g.add((end_date_node, URIRef(crm + "P4_has_time-span"), end_date_time_span))
            g = create_time_span_tripels(
                'end', end_date_time_span, organization, g)
            # if len(res['end_date_written']) == 4 and res['end_end_date'] is not None:
            #     # check whether only a year has bin given for the start date and add according nodes
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_start_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_end_date']}T23:59:59", datatype=XSD.dateTime))))
            # else:
            #     g.add((end_date_time_span, crm.P82a_begin_of_the_begin, (Literal(
            #         f"{res['end_date']}T00:00:00", datatype=XSD.dateTime))))
            #     g.add((end_date_time_span, crm.P82b_end_of_the_end, (Literal(
            #         f"{res['end_date']}T23:59:59", datatype=XSD.dateTime))))
    return g


def render_event(event, event_type, node_event, g):
    """renders event object as RDF graph

    Args:
        event (_type_): _description_
        g (_type_): _description_
    """
    # prepare basic node types
    # node_event = URIRef(f"{idmapis}{event_type}/{event['id']}")
    node_event_role = URIRef(f"{idmapis}{event_type}/eventrole/{event['id']}")
    node_pers = URIRef(f"{idmapis}personproxy/{event['related_person']['id']}")
    node_roletype = URIRef(f"{idmrole}{event['relation_type']['id']}")
    g.add((node_event_role, bioc.inheres_in, node_pers))
    g.add((node_event_role, RDF.type, node_roletype))
    g.add((node_roletype, rdfs.subClassOf, bioc.Event_Role))
    g.add((node_roletype, RDFS.label, Literal(
        event['relation_type']['label'])))
    # suggestion to add specific event role
    g.add((node_event, bioc.had_participant_in_role, node_event_role))
    # connect event and event role
    g.add((node_event, RDF.type, crm.E5_Event))
    # define crm classification
    g.add((node_event_role, RDFS.label, Literal(
        event['relation_type']['label'])))
    g.add((node_event, RDFS.label, Literal(
        f"{event['related_person']['label']} {event['relation_type']['label']} {event['related_place']['label']}")))
    if event['start_date'] is not None:
        node_timespan = URIRef(f"{idmapis}{event_type}/timespan/{event['id']}")
        g.add((node_event, URIRef(crm + "P4_has_time-span"), node_timespan))
        # add time-span to event
        g = create_time_span_tripels('start', node_timespan, event, g)
        # add end of time-span
        if event['end_date'] is not None:
            g = create_time_span_tripels('end', node_timespan, event, g)
    return g


@task()
def render_place(place, g, base_uri):
    """renders place object as RDF graph

    Args:
        place (_type_): _description_
        g (_type_): _description_
    """
#    res = await get_entity(place, 'place')
    # setup basic nodes
    if place is None:
        return g
    node_place = URIRef(f"{idmapis}place/{place['id']}")
    g.add((node_place, RDFS.label, Literal(place['name'])))
    node_appelation = URIRef(f"{idmapis}placeappellation/{place['id']}")
    node_plc_identifier = URIRef(f"{idmapis}placeidentifier/{place['id']}")

    g.add((node_place, RDF.type, crm.E53_Place))
    # define place as Cidoc E53 Place
    g.add((node_place, crm.P1_is_identified_by, node_appelation))
    # add appellation to place
    g.add((node_appelation, RDF.type, crm.E33_E41_Linguistic_Appellation))
    # define appellation as linguistic appellation
    g.add((node_appelation, RDFS.label, Literal(place['name'])))
    # add label to appellation
    g.add((node_place, owl.sameAs, URIRef(
        f"{base_uri}/entity/{place['id']}")))
    for uri in place['sameAs']:
        g.add((node_place, owl.sameAs, URIRef(uri)))
    g.add((node_place, crm.P1_is_identified_by, URIRef(
        f"{idmapis}placeidentifier/{place['id']}")))
    # add APIS Identifier as Identifier
    g.add((node_plc_identifier, RDF.type, crm.E_42_Identifier))
    # define APIS Identifier as E42 Identifier (could add a class APIS-Identifier or model a Identifier Assignment Event)
    g.add((node_plc_identifier, RDFS.label, Literal(place['id'])))
    # add label to APIS Identifier
    # define that individual in APIS named graph and APIS entity are the same
    if place['lat'] is not None and place['lng'] is not None:
        node_spaceprimitive = URIRef(f"{idmapis}spaceprimitive/{place['id']}")
        g.add((node_place, crm.P168_place_is_defined_by, node_spaceprimitive))
        g.add((node_spaceprimitive, rdf.type, crm.E94_Space_Primitive))
        g.add((node_spaceprimitive, crm.P168_place_is_defined_by, Literal(
            (f"Point ( {'+' if place['lng'] > 0 else ''}{place['lng']} {'+' if place['lat'] > 0 else ''}{place['lat']} )"), datatype=geo.wktLiteral)))
        # define that individual in APIS named graph and APIS entity are the same
    # suggestion for serialization of space primitives according to ISO 6709, to be discussed
    # more place details will be added (references, source, place start date, place end date, relations to other places(?))
    return g


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_persons(base_uri, filter_params):
    """gets persons from API

    Args:
        filter_params (_type_): _description_
    """
    logger = prefect.context.get("logger")
    loop_payload = prefect.context.get("task_loop_result", {})
    res_fin = loop_payload.get("res_fin", [])
    next_url = loop_payload.get(
        "next_url", f"{base_uri}/apis/api/entities/person")
    if "limit" not in filter_params:
        filter_params["limit"] = 100
    if "format" not in filter_params:
        filter_params["format"] = "json"
    res = requests.get(next_url, params=filter_params)
    if res.status_code != 200:
        logger.warn(f"Error getting persons: {res.status_code} / {res.text}")
        raise RETRY
    res = res.json()
    res_fin.extend(res["results"])
    if res["next"] is not None:
        raise LOOP(message=f"offset {res['offset']}", result={
            "res_fin": res_fin, "next_url": res["next"]})
    return res_fin


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_entity(entity_id, entity_type, base_uri):
    """gets organization object from API

    Args:
        organization_id (_type_): _description_
    """
    logger = prefect.context.get("logger")
    if entity_id is None:
        logger.info("Entity is None, skipping")
        raise SKIP
    params = {"format": "json", "include_relations": "false"}
    res = requests.get(
        f"{base_uri}/apis/api/entities/{entity_type}/{entity_id}/", params=params)
    if res.status_code != 200:
        logger.warn(
            f"Error getting {entity_type}: {res.status_code} / {res.text}")
        raise RETRY
    else:
        return res.json()


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_entity_relations(base_uri, entity, kind, related_entity_type):
    """gets entity relations from API

    Args:
        person_id (_type_): _description_
    """
    logger = prefect.context.get("logger")
    loop_payload = prefect.context.get("task_loop_result", {})
    if entity is None:
        logger.info("Entity is None, skipping")
        raise SKIP
    elif isinstance(entity, int):
        entity = {"id": entity}
    entity_id = entity["id"]
    url = loop_payload.get(
        "next_url", f"{base_uri}/apis/api/relations/{kind}/")
    res_full = loop_payload.get("res_full", [])
    query_params = {f"related_{related_entity_type}": entity_id}
    if kind[:int(len(kind)/2)] == kind[int(len(kind)/2):]:
        del query_params[f"related_{related_entity_type}"]
        query_params[f"related_{related_entity_type}A"] = entity_id
    res = requests.get(url, params=query_params)
    if res.status_code != 200:
        logger.error(f"Error getting {kind} for entity {entity_id}, retrying")
        raise RETRY
    res = res.json()
    res_full.extend(res["results"])
    if res["next"] is not None:
        raise LOOP(message=f"offset {res['offset']}", result={
            "res_full": res_full, "next_url": res["next"]})
    return res_full


@task()
def create_base_graph(base_uri):
    global crm
    crm = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
    """Defines namespace for CIDOC CRM."""
    global ex
    ex = Namespace('http://www.intavia.eu/')
    """Defines namespace for own ontology."""
    global idmcore
    idmcore = Namespace('http://www.intavia.eu/idm-core/')
    """Defines namespace for own ontology."""
    global idmrole
    idmrole = Namespace('http://www.intavia.eu/idm-role/')
    """Defines namespace for role ontology."""
    global idmapis
    idmapis = Namespace('http://www.intavia.eu/apis/')
    """Namespace for InTaVia named graph"""
    global idmbibl
    idmbibl = Namespace('http://www.intavia.eu/idm-bibl/')
    """Namespace for bibliographic named graph"""
    global idmrelations
    idmrelations = Namespace('http://www.intavia.eu/idm-relations')
    """Defines namespace for relation ontology."""
    global intavia_shared
    intavia_shared = Namespace('http://www.intavia.eu/shared-entities')
    """Defines namespace for relation ontology."""
    global ore
    ore = Namespace('http://www.openarchives.org/ore/terms/')
    """Defines namespace for schema.org vocabulary."""
    global edm
    edm = Namespace('http://www.europeana.eu/schemas/edm/')
    """Defines namespace for Europeana data model vocabulary."""
    global owl
    owl = Namespace('http://www.w3.org/2002/7/owl#')
    """Defines namespace for Europeana data model vocabulary."""
    global rdf
    rdf = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    """Defines namespace for Europeana data model vocabulary."""
    global xml
    xml = Namespace('http://www.w3.org/XML/1998/namespace')
    """Defines namespace for Europeana data model vocabulary."""
    global xsd
    xsd = Namespace('http://www.w3.org/2001/XMLSchema#')
    """Defines namespace for Europeana data model vocabulary."""
    global bioc
    bioc = Namespace('http://ldf.fi/schema/bioc/')
    """Defines namespace for Europeana data model vocabulary."""
    global rdfs
    rdfs = Namespace('http://www.w3.org/2000/01/rdf-schema#')
    """Defines namespace for Europeana data model vocabulary."""
    global apis
    apis = Namespace(base_uri)
    """Defines namespace for APIS database."""
    global bf
    bf = Namespace('http://id.loc.gov/ontologies/bibframe/')
    """Defines bibframe namespace."""
    global geo
    geo = Namespace("http://www.opengis.net/ont/geosparql#")
    global g
    g = Graph()
    g.bind("apis", apis)
    g.bind('crm', crm)
    g.bind('intaviashared', intavia_shared)
    g.bind('ore', ore)
    g.bind('edm', edm)
    g.bind('owl', owl)
    g.bind('rdf', rdf)
    g.bind('xml', xml)
    g.bind('xsd', xsd)
    g.bind('bioc', bioc)
    g.bind('rdfs', rdfs)
    g.bind('apis', apis)
    g.bind('idmcore', idmcore)
    g.bind('idmrole', idmrole)
    g.bind('idmrelations', idmrelations)
    g.bind('owl', owl)
    g.bind("geo", geo)
    g.bind("bf", bf)
    g.bind("ex", ex)
    g.bind("idmbibl", idmbibl)
    g.bind("idmapis", idmapis)
    global glob_list_entities
    glob_list_entities = {"institutions": [], "persons": [], "places": []}
    return g


@task
def serialize_graph(g, storage_path, named_graph):
    Path(storage_path).mkdir(parents=True, exist_ok=True)
    for s, p, o in g.triples((None, bioc.inheres_in, None)):
        g.add((o, bioc.bearer_of, s))
    f_path = f"{storage_path}/apis_data_{datetime.now().strftime('%d-%m-%Y')}.ttl"
    g.serialize(
        destination=f_path, format="turtle", )
    return f_path


filter_results = FilterTask(
    filter_func=lambda x: not isinstance(x, (BaseException, SKIP, type(None)))
)


@task
def upload_data(f_path, named_graph, sparql_endpoint=None):
    logger = prefect.context.get("logger")
    data = open(f_path, 'rb').read()
    headers = {
        "Content-Type": "application/x-turtle",
    }
    params = {'context-uri': named_graph}
    if sparql_endpoint is None:
        sparql_endpoint = "https://triplestore.acdh-dev.oeaw.ac.at/intavia/sparql"
    logger.info(
        f"Uploading data to {sparql_endpoint} with params {params}, loading file from {f_path}")
    upload = requests.post(sparql_endpoint, data=data, headers=headers, params=params, auth=requests.auth.HTTPBasicAuth(
        os.environ.get('RDFDB_USER'), os.environ.get('RDFDB_PASSWORD')))
    logger.info(f"Upload status: {upload.status_code}")
    if upload.status_code != 200:
        raise FAIL(f"Upload failed with status code {upload.status_code}")


with Flow("Create RDF from APIS API") as flow:
    max_entities = Parameter("Max Entities", default=None)
    named_graph = Parameter(
        "Named Graph", default="http://data.acdh.oeaw.ac.at/intavia/cho")
    endpoint = Parameter("Base URL of APIS endpoint",
                         default="https://apis.acdh.oeaw.ac.at")
    base_uri_serialization = Parameter(
        "Base URI used in serialization", default="https://apis.acdh.oeaw.ac.at")
    filter_params = Parameter("Filter Parameters", default={"collection": 86})
    storage_path = Parameter(
        "Storage Path", default="/archive/serializations/APIS")
    g = create_base_graph(base_uri_serialization)
    persons = get_persons(endpoint, filter_params)
    person_return = render_person.map(persons, unmapped(
        g), unmapped(base_uri_serialization))
    pers_inst = get_entity_relations.map(unmapped(endpoint),
                                         persons, kind=unmapped('personinstitution'), related_entity_type=unmapped("person"))
    pers_place = get_entity_relations.map(unmapped(endpoint),
                                          persons, kind=unmapped('personplace'), related_entity_type=unmapped("person"))
    places_data_persons = render_personplace_relation.map(
        flatten(pers_place), unmapped(g), unmapped(base_uri_serialization))
    places_data_persons_filtered = filter_results(places_data_persons)
    pers_pers = get_entity_relations.map(
        unmapped(endpoint), persons, kind=unmapped('personperson'), related_entity_type=unmapped("person"))
    additional_persons = render_personperson_relation.map(
        pers_pers, unmapped(g))
    additional_persons_filtered = filter_results(additional_persons)
    additional_persons_data = get_entity.map(
        additional_persons_filtered, unmapped('person'), unmapped(endpoint))
    additional_persons_return = render_person.map(
        additional_persons_data, unmapped(g), unmapped(base_uri_serialization))
    insts = render_personinstitution_relation.map(
        flatten(pers_inst), unmapped(g))
    outp_persrole = render_personrole.map(
        flatten(pers_inst), unmapped(g), unmapped(base_uri_serialization))
    insts_data = get_entity.map(flatten(insts), unmapped(
        'institution'), unmapped(endpoint))
    orga_return = render_organization.map(
        insts_data, unmapped(g), unmapped(base_uri_serialization))
    insts_rel_data = get_entity_relations.map(unmapped(endpoint), flatten(insts), kind=unmapped(
        "institutionplace"), related_entity_type=unmapped("institution"), )
    places_data_institutions = render_organizationplace_relation.map(
        flatten(insts_rel_data), unmapped(g))
    places_data_institutions_filtered = filter_results(
        places_data_institutions)
    places_data_fin = get_entity.map(flatten(
        places_data_persons_filtered + places_data_institutions_filtered), unmapped('place'), unmapped(endpoint))
    places_data_fin_filtered = filter_results(places_data_fin)
    places_out = render_place.map(places_data_fin_filtered, unmapped(
        g), unmapped(base_uri_serialization))
    places_out_filtered = filter_results(places_out)
    out = serialize_graph(
        g, storage_path, named_graph, upstream_tasks=[places_out_filtered])
    upload_data(out, named_graph, upstream_tasks=[out])
# state = flow.run(executor=LocalExecutor(), parameters={
#                 'Filter Parameters': {"collection": 86, 'first_name': 'Markus'}, 'Storage Path': '/workspaces/prefect-flows'})
flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "requests rdflib", },
                                job_template_path="https://raw.githubusercontent.com/InTaVia/prefect-flows/master/intavia-job-template.yaml")
flow.storage = GitHub(repo="InTaVia/prefect-flows",
                      path="create_apis_graph_v1.py")
