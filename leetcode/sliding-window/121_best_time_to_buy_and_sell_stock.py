class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        maxProfit = 0
        currStockPrice = float("inf")
        end = 0
        while end < len(prices):
            currStockPrice = min(currStockPrice, prices[end])
            maxProfit = max(maxProfit, prices[end] - currStockPrice)
            end += 1

        return maxProfit
