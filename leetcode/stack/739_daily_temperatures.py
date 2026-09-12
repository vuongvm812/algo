class Solution:
    def dailyTemperatures(self, temperatures: List[int]) -> List[int]:
        res = [0] * len(temperatures)
        stack = []
        for i in range(len(temperatures)):
            temp = temperatures[i]
            if len(stack) == 0 or temperatures[stack[-1]] >= temp:
                stack.append(i)
            if temperatures[stack[-1]] < temp:
                while len(stack) > 0 and temperatures[stack[-1]] < temp:
                    pos = stack.pop()
                    res[pos] = i - pos
                stack.append(i)

        return res
