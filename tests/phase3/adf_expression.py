"""Strict offline evaluator for the ADF expression subset used by these artifacts.

Not an ADF runtime replacement: network, SQL compilation and service behavior are
intentionally outside its scope. Unknown functions or missing properties fail.
"""
import datetime
import json
import re

TOKEN = re.compile(r"\s*('(?:''|[^'])*'|[A-Za-z_][A-Za-z_0-9]*|\d+|[(),.])")


def evaluate(expression, context):
    if isinstance(expression, dict):
        expression = expression['value']
    if not expression.startswith('@') or expression.startswith('@{'):
        raise ValueError('Expected a typed expression, not string interpolation')
    text, tokens, offset = expression[1:], [], 0
    while offset < len(text):
        match = TOKEN.match(text, offset)
        if not match:
            raise ValueError('Invalid expression token: ' + text[offset:offset + 20])
        tokens.append(match[1])
        offset = match.end()
    index = 0

    def take(expected=None):
        nonlocal index
        if index == len(tokens):
            raise ValueError('Unexpected end of expression')
        value = tokens[index]
        index += 1
        if expected is not None and value != expected:
            raise ValueError('Expected ' + expected + ', got ' + value)
        return value

    def boolean(value):
        if type(value) is not bool:
            raise TypeError('ADF condition requires Bool, not ' + type(value).__name__)
        return value

    def integer(value):
        if type(value) is bool or not re.fullmatch(r'-?\d+', str(value)):
            raise ValueError('Not an integer: ' + str(value))
        result = int(value)
        if not -(2**63) <= result < 2**63:
            raise ValueError('ADF integer overflow')
        return result

    def call(name, args):
        if name in ['pipeline', 'item', 'dataset', 'utcNow']:
            if args:
                raise ValueError('Unexpected arguments')
            return context[name]
        if name == 'variables':
            return context['variables'][args[0]]
        if name == 'activity':
            return context['activities'][args[0]]
        if name == 'concat':
            if not all(isinstance(a, str) for a in args):
                raise TypeError('concat needs strings')
            return ''.join(args)
        if name == 'int':
            return integer(args[0])
        if name == 'string':
            return json.dumps(args[0], separators=(',', ':')) if not isinstance(args[0], str) else args[0]
        if name == 'json':
            return json.loads(args[0])
        if name == 'if':
            return args[1] if boolean(args[0]) else args[2]
        if name == 'equals':
            return type(args[0]) is type(args[1]) and args[0] == args[1]
        if name in ['and', 'or']:
            if len(args) != 2:
                raise ValueError('Boolean functions require two arguments')
            values = [boolean(a) for a in args]
            return all(values) if name == 'and' else any(values)
        if name in ['greaterOrEquals', 'lessOrEquals']:
            return args[0] >= args[1] if name == 'greaterOrEquals' else args[0] <= args[1]
        if name == 'contains':
            return args[1] in args[0]
        if name == 'createArray':
            return args
        if name == 'length':
            return len(args[0])
        if name == 'toLower':
            return args[0].lower()
        if name == 'formatDateTime':
            if args[1] != 'yyyy-MM-dd':
                raise ValueError('Unexpected date format')
            return datetime.datetime.fromisoformat(args[0].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        raise ValueError('Unsupported ADF function: ' + name)

    def value():
        token = take()
        if token.startswith("'"):
            result = token[1:-1].replace("''", "'")
        elif token.isdigit():
            result = int(token)
        elif token in ['true', 'false']:
            result = token == 'true'
        else:
            take('(')
            args = []
            if tokens[index] != ')':
                args.append(value())
                while tokens[index] == ',':
                    take(',')
                    args.append(value())
            take(')')
            result = call(token, args)
        while index < len(tokens) and tokens[index] == '.':
            take('.')
            result = result[take()]
        return result

    result = value()
    if index != len(tokens):
        raise ValueError('Unconsumed expression tokens')
    return result


def condition_status(activity, context):
    """Model branch selection and propagation of nested Fail for these gates."""
    if activity['type'] != 'IfCondition':
        raise ValueError('Expected IfCondition')
    result = evaluate(activity['typeProperties']['expression'], context)
    if type(result) is not bool:
        raise TypeError('IfCondition expression did not return Bool')
    branch = activity['typeProperties']['ifTrueActivities' if result else 'ifFalseActivities']
    if any(node['type'] == 'Fail' for node in branch):
        return 'Failed'
    if branch:
        raise ValueError('Unsupported test branch; extend model explicitly')
    return 'Succeeded'
