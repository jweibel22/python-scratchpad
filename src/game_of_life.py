

class State:
    def __init__(self, size):
        self.size = size
        self.cells = [[False for i in range(size)] for i in range(size)]


def count_neighbours(state, x, y):
    count = 0
    for i in range(max(0, x-1), min(x+1, state.size-1)+1):
        for j in range(max(0, y - 1), min(y + 1, state.size-1)+1):
            if i == x and j == y:
                continue
            print('checking: x={x},y={y},i={i},j={j},alive={alive}'.format(x=x, y=y, i=i, j=j, alive=state.cells[i][j]))
            if state.cells[i][j]:
                count += 1

    return count


def tick(state):
    result = State(state.size)
    for i in range(state.size):
        for j in range(state.size):
            neigbours = count_neighbours(state, i, j)
            if state.cells[i][j]:
                result.cells[i][j] = neigbours == 2 or neigbours == 3
            else:
                result.cells[i][j] = neigbours == 3
    return result


def to_html(state):
    table = '<table border=1>'
    for i in range(len(state.cells)):
        table += '<tr>'
        for j in range(len(state.cells[i])):
            color = 'black' if state.cells[i][j] else 'white'
            table += '<td width=20 height=20 bgcolor={color}></td>'.format(color=color)
        table += '</tr>'

    table += '</table>'
    return table


def xxx(states):
    result = ['<div><p>{body}</div>'.format(body=to_html(state)) for state in states]
    return '<html><body>{body}</body></html>'.format(body=''.join(result))


state = State(5)
state.cells[1][1] = True
state.cells[1][2] = True
state.cells[1][3] = True
state.cells[2][3] = True
state.cells[3][2] = True

states = [state]
for i in range(1, 5):
    states.append(tick(states[i-1]))

html = xxx(states)

f = open('gol.html', 'w')
f.write(html)
f.close()
