class Solution:
    def searchMatrix(self, matrix: List[List[int]], target: int) -> bool:
        left, right = 0, len(matrix) * len(matrix[0]) - 1
        while left <= right:
            mid = left + (right - left) // 2
            r, c = mid // len(matrix[0]), mid % len(matrix[0])
            if matrix[r][c] == target:
                return True
            if matrix[r][c] < target:
                left = mid + 1
            else:
                right = mid - 1

        return False
