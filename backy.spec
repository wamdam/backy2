# -*- mode: python -*-

block_cipher = None

a = Analysis(['src/backy2/scripts/backy.py'],
             pathex=['/home/dk/develop/backy2'],
             binaries=[],
             datas=[
                ('src/backy2/meta_backends/sql_migrations/alembic.ini', 'backy2/meta_backends/sql_migrations'),
                ('src/backy2/meta_backends/sql_migrations/alembic/*', 'backy2/meta_backends/sql_migrations/alembic'),
                ('src/backy2/meta_backends/sql_migrations/alembic/versions/*', 'backy2/meta_backends/sql_migrations/alembic/versions'),
             ],
             hiddenimports=[
                'backy2.meta_backends',
                'backy2.meta_backends.sql',
                'backy2.meta_backends.sql_migrations',
                'backy2.meta_backends.sql_migrations.alembic.env',
                'backy2.data_backends',
                'backy2.data_backends.file',
                'backy2.data_backends.s3',
             ],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='backy',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )
