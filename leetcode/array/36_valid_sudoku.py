class Solution:
    def isValidSudoku(self, board: List[List[str]]) -> bool:
        for i in range(9):
            visitedRow = set()
            visitedCol = set()
            visitedDiag = set()
            for j in range(9):
                row = board[i][j]
                col = board[j][i]
                diag = board[3 * (i // 3) + j // 3][3 * (i % 3) + j % 3]

                if row != ".":
                    if row in visitedRow:
                        return False
                    else:
                        visitedRow.add(row)

                if col != ".":
                    if col in visitedCol:
                        return False
                    else:
                        visitedCol.add(col)

                if diag != ".":
                    if diag in visitedDiag:
                        return False
                    else:
                        visitedDiag.add(diag)

        return True
