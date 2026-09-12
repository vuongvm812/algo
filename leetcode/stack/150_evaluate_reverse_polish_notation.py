class Solution:
    def evalRPN(self, tokens: List[str]) -> int:
        storeNum = []
        for ch in tokens:
            if ch == "+":
                a = storeNum.pop()
                b = storeNum.pop()
                storeNum.append(a + b)
            elif ch == "-":
                a = storeNum.pop()
                b = storeNum.pop()
                storeNum.append(b - a)
            elif ch == "*":
                a = storeNum.pop()
                b = storeNum.pop()
                storeNum.append(a * b)
            elif ch == "/":
                a = storeNum.pop()
                b = storeNum.pop()
                storeNum.append(int(float(b) / a))
            else:
                storeNum.append(int(ch))

        return int(storeNum[0])
