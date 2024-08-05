import json

from sqlalchemy import text


class FaceManager(object):

    def __init__(self, session):
        self.session = session

    def get_faces_feature(self, repo_id):
        with self.session() as session:
            sql = "SELECT face_id, feature FROM repo_image_face_feature WHERE repo_id='%s'" % repo_id
            faces = session.execute(text(sql)).fetchall()

        return [{
            'face_id': face[0],
            'feature': json.loads(face[1])
        } for face in faces]

    def insert_face_feature(self, repo_id, face_id, feature):
        with self.session() as session:
            session.execute(
                text("INSERT INTO repo_image_face_feature (`repo_id`, `face_id`, `feature`) VALUES (:repo_id, :face_id, :feature)"),
                {'repo_id': repo_id, 'face_id': face_id, 'feature': feature})
            session.commit()

    def delete_face_feature(self, repo_id, face_id):
        with self.session() as session:
            session.execute(
                text("DELETE FROM repo_image_face_feature WHERE repo_id=:repo_id AND face_id=:face_id"),
                {'repo_id': repo_id, 'face_id': face_id})
            session.commit()

    def get_face(self, repo_id, path):
        with self.session() as session:
            sql = "SELECT face_id FROM repo_image_face WHERE repo_id='%s' AND path='%s'" % (repo_id, path)
            res = session.execute(text(sql)).fetchone()

        return res

    def get_path_by_face_id(self, repo_id, face_id):
        with self.session() as session:
            sql = "SELECT * FROM repo_image_face WHERE repo_id='%s' AND face_id='%s'" % (repo_id, face_id)
            res = session.execute(text(sql)).fetchone()

        return res

    def insert_faces(self, repo_id, face_id, path):
        with self.session() as session:
            session.execute(
                text("INSERT INTO repo_image_face (`repo_id`, `face_id`, `path`) VALUES (:repo_id, :face_id, :path)"),
                {'repo_id': repo_id, 'face_id': face_id, 'path': path})
            session.commit()

    def update_faces(self, repo_id, face_id, path):
        with self.session() as session:
            session.execute(
                text("UPDATE repo_image_face SET face_id=:face_id WHERE repo_id=:repo_id AND path=:path"),
                {'repo_id': repo_id, 'face_id': face_id, 'path': path})
            session.commit()

    def delete_faces(self, repo_id, path):
        with self.session() as session:
            session.execute(
                text("DELETE FROM repo_image_face WHERE repo_id=:repo_id AND path=:path"),
                {'repo_id': repo_id, 'path': path})
            session.commit()
