import json

from flask import jsonify
from sqlalchemy import and_
from sqlalchemy import text, not_
from sqlalchemy.exc import SQLAlchemyError


class ApiView:
    def __init__(self, class_instance, identifier_attr, relationships, db, on_data_change=None):
        self.instance = class_instance
        self.id = identifier_attr
        self.relationships = relationships
        self.db = db
        self.on_data_change = on_data_change

    def list_entries(self,  fields, sorts, offset, quantity):
        queries = []
        applied_sorts = ''
        object_queries = []

        for i in fields:
            if i.get('type', None) == 'string':
                if i.get('different_from', False):  # DIFFERENT
                    queries.append(not_(getattr(self.instance, i.get('key', None)) == i.get('value', None)))
                elif i.get('equal_to', False):  # EQUALS
                    queries.append(getattr(self.instance, i.get('key', None)) == i.get('value', None))
                elif i.get('contains', False):  # CONTAINS
                    queries.append(getattr(self.instance, i.get('key', None)).ilike('%' + i.get('value', None) + '%'))
            elif i.get('type', None) == 'date' or i.get('type', None) == 'number':  # FILTER BY DATE OR NUMBER
                if i.get('less_than', False):  # LESS THAN THE NUMBER/DATE
                    queries.append(getattr(self.instance, i.get('key', None)) <= i.get('value', None))
                elif i.get('greater_than', False):  # MORE THAN THE NUMBER/DATE
                    queries.append(getattr(self.instance, i.get('key', None)) >= i.get('value', None))
                elif i.get('equal_to', False):  # EQUAL TO THE NUMBER/DATE
                    queries.append(getattr(self.instance, i.get('key', None)) == i.get('value', None))
            else:
                if i.get('type', None) == 'object' and i.get('sub_relation', None) is not None:
                    object_queries.append(i)
                else:
                    if i.get('different_from', False):
                        queries.append(getattr(self.instance, i.get('key', None)) != i.get('value', None))
                    else:
                        queries.append(getattr(self.instance, i.get('key', None)) == i.get('value', None))
        for index, i in enumerate(sorts):
            end = '' if index == (len(sorts) - 1) else ', '
            if i.get('desc', False):
                applied_sorts += getattr(self.instance, i.get('key', None)).name + ' DESC' + end
            else:
                applied_sorts += getattr(self.instance, i.get('key', None)).name + ' ASC' + end

        if not queries:
            query = self.db.session.query(self.instance)
        else:
            query = self.db.session.query(self.instance).filter(and_(*queries))

        if len(self.relationships) > 0:
            already_joined = []
            for current_query in object_queries:
                relations = []
                current = current_query
                while current is not None:
                    relations.append(current.get('key', None))
                    current = current.get('sub_relation', None)

                last_relation = None
                joined = []
                for i in relations:
                    for j in self.relationships:
                        if j.get('key') == i and j.get('key') not in already_joined:
                            already_joined.append(j.get('key'))
                            joined.append(j.get('key'))
                            query = query.join(j.get('instance', None))
                        if last_relation is None and j.get('key') == relations[len(relations) - 2]:
                            last_relation = j.get('instance')

                if last_relation is not None:
                    query = query.filter(
                        getattr(last_relation, relations[len(relations) - 1]) == current_query.get('value', None))

        if not queries:
            query = query.order_by(text(applied_sorts)).offset(offset).limit(quantity)
        else:
            query = query.order_by(text(applied_sorts)).offset(offset).limit(quantity)
        return query

    def __find_relationship(self, key):
        response = None
        for i in self.relationships:
            if i.get('key', None) == key:
                response = i
        return response

    def parse_entry(self, entry):
        fields = []
        data = entry.__dict__
        for i in data:
            fields.append(i)

        for i in fields:
            relation = self.__find_relationship(i)

            if relation is not None and relation.get('instance', None) is not None:

                # try:
                entity = self.db.session.get(relation.get('instance', None), data[relation.get('key', None)])

                if entity is not None:
                    obj = entity.__dict__
                    obj = {k: v for (k, v) in obj.items()
                         if k != '_sa_instance_state'}

                    data[relation.get('key', None)] = obj

        data = {k: v for (k, v) in data.items()
               if k != '_sa_instance_state'}
        return data

    def put(self, entity_id, package=None):
        if package is not None:
            try:
                entry = self.instance.query.get(entity_id)

                if entry is not None:
                    for i in package:
                        if hasattr(entry, i) and i != self.id:
                            setattr(entry, i, package.get(i, None))

                    self.db.session.commit()

                    new_instance = self.parse_entry(self.instance.query.get(data.get(self.id, None)))
                    if self.on_data_change is not None:
                        self.on_data_change(new_instance, 'put')
                    return jsonify({'status': 'success', 'description': 'accepted', 'code': 202}), 202
                else:
                    return jsonify({'status': 'error', 'description': 'not_found', 'code': 404}), 404
            except SQLAlchemyError as e:
                return jsonify({'status': 'error', 'description': str(e), 'code': 400}), 400
        else:
            return jsonify({'status': 'error', 'description': 'No data', 'code': 400}), 400

    def post(self, package=None):
        try:
            entry = self.instance(package)
            if self.on_data_change is not None:
                self.on_data_change(self.parse_entry(entry), 'post')
            return jsonify(self.parse_entry(entry)), 201
        except SQLAlchemyError as e:
            return jsonify({'status': 'error', 'description': str(e), 'code': 400}), 400

    def get(self, entity_id):
        try:
            entry = self.instance.query.get(entity_id)
            if entry is not None:
                return jsonify(self.parse_entry(entry)), 200
            else:
                return jsonify({'status': 'error', 'description': 'not_found', 'code': 404}), 404
        except SQLAlchemyError as e:
            return jsonify({'status': 'error', 'description': str(e), 'code': 400}), 400

    def delete(self, entity_id):
        try:
            entry = self.instance.query.get(entity_id)
            if entry is not None:
                self.db.session.delete(entry)
                self.db.session.commit()
                if self.on_data_change is not None:
                    self.on_data_change({self.id: entity_id}, 'delete')
                return jsonify({'status': 'success', 'description': 'no_content', 'code': 206}), 206
            else:
                return jsonify({'status': 'error', 'description': 'not_found', 'code': 404}), 404

        except SQLAlchemyError as e:
            return jsonify({'status': 'error', 'description': str(e), 'code': 400}), 400

    def list(self, data, base_query=[]):

        if data is None:
            data = dict()
        fields = data.get('filters', [])
        sorts = data.get('sorts', [])

        if fields is not None and type(fields) == str:
            fields = json.loads(fields)
        if sorts is not None and type(sorts) == str:
            sorts = json.loads(sorts)

        for i in base_query:
            fields.append(i)

        response = []
        query = self.list_entries(

            fields=fields,
            sorts=sorts,
            quantity=data.get('quantity', 15),
            offset=(int(data.get('page', 0)) * int(data.get('quantity', 15)))
        )
        for i in query:
            response.append(self.parse_entry(i))

        return jsonify(response), 200
