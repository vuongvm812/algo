class Solution:
    def isValid(self, s: str) -> bool:
        stack = []
        for ch in s:
            if len(stack) == 0 or ch == "(" or ch == "{" or ch == "[":
                stack.append(ch)
            elif (
                (ch == ")" and stack[-1] == "(")
                or (ch == "}" and stack[-1] == "{")
                or (ch == "]" and stack[-1] == "[")
            ):
                stack.pop()
            else:
                return False

        return len(stack) == 0
