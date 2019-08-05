from socceraction.spadl.config import *
import socceraction.spadl.opta as _opta
import socceraction.spadl.statsbomb as _sb
import socceraction.spadl.wyscout as _wyscout

optajson_to_optah5 = _opta.jsonfiles_to_h5
optah5_to_spadlh5 = _opta.convert_to_spadl

statsbombjson_to_statsbombh5 = _sb.jsonfiles_to_h5
statsbombh5_to_spadlh5 = _sb.convert_to_spadl

wyscoutjson_to_wyscouth5 = _wyscout.jsonfiles_to_h5
wyscouth5_to_spadlh5 = _wyscout.convert_to_spadl

