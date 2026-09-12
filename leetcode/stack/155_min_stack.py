class Node:
    def __init__(self, val, minVal):
        self.val = val
        self.minVal = minVal


class MinStack:
    def __init__(self):
        self.stack = []

    def push(self, value: int) -> None:
        minVal = value
        if len(self.stack) != 0:
            minVal = min(minVal, self.stack[-1].minVal)
        self.stack.append(Node(value, minVal))

    def pop(self) -> None:
        self.stack.pop()

    def top(self) -> int:
        return self.stack[-1].val

    def getMin(self) -> int:
        return self.stack[-1].minVal
