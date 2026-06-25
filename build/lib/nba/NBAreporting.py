from .NBAbase import base


class reporting(base):

    def __init__(self):
        super().__init__()

    def showPlayerLog(self,names,lastNGames = 10,type='Base'):
        '''
        Show player (list of names) logs up to n games with the default being 10.
        Have a base layout that will give points, 2 pt shots, 3 pt shots, ft, possessions, off rating, team off rating,
        opponent and opponent rating.
        There will be a threes focus, first basket and points
        '''
