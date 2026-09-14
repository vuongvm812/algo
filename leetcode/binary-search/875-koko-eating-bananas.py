class Solution:
    def minEatingSpeed(self, piles: List[int], h: int) -> int:
        left, right = 1, max(piles)
        while left <= right:
            mid = left + (right - left) // 2
            hour = 0
            for pile in piles:
                hour += math.ceil(pile / mid)
            if hour <= h:
                right = mid - 1
            else:
                left = mid + 1

        return left
