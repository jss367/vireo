"""Independent synthetic database edge cases; local validation only."""
import json

import pytest
from test_db import _raw_jpeg_pair_db


@pytest.mark.parametrize('companion', ['paired-no-suffix', '', None])
@pytest.mark.parametrize('op,value', [('is not','.jpg'),('not_in',['.jpg'])])
def test_negative_extension_with_extensionless_companion(tmp_path, companion, op, value):
    db=_raw_jpeg_pair_db(tmp_path)
    db.conn.execute('UPDATE photos SET companion_path=? WHERE filename=?',(companion,'_D854674.NEF'))
    db.conn.commit()
    cid=db.add_collection('negative',json.dumps([{'field':'extension','op':op,'value':value}]))
    assert sorted(p['filename'] for p in db.get_collection_photos(cid))==['_D854674.NEF','lone.nef']

@pytest.mark.parametrize('companion', ['bird.edit.JPG','bird ä.版本.JpG','bird.JPG'])
def test_multi_dot_unicode_and_same_format_count(tmp_path, companion):
    db=_raw_jpeg_pair_db(tmp_path)
    db.conn.execute('UPDATE photos SET companion_path=? WHERE filename=?',(companion,'_D854674.NEF'))
    db.conn.execute("UPDATE photos SET companion_path='other.JPG' WHERE filename='lone.jpg'")
    db.conn.commit()
    assert db.count_photos_for_rules([{'field':'extension','op':'is','value':'.jpg'}])==2
    values={v['value']:v['count'] for v in db.get_filter_field_values('extension')}
    assert values=={'.jpg':2,'.nef':2}
    assert db.get_workspace_extensions()==['.jpg','.nef']
