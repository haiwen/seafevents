import cv2
import insightface
import numpy as np
from sklearn import preprocessing


class FaceRecognition:
    def __init__(self, config, gpu_id=0, threshold=1.24, det_thresh=0.50):
        self.AI_section_name = 'AI'
        if config.has_section(self.AI_section_name) and config.has_option(self.AI_section_name, 'face_recognition_model_path'):
            self.model_path = config.get(self.AI_section_name, 'face_recognition_model_path')
        else:
            self.model_path = '~/.insightface'
        self.gpu_id = gpu_id
        self.threshold = threshold
        self.det_thresh = det_thresh

        self.model = insightface.app.FaceAnalysis(root=self.model_path)
        self.model.prepare(ctx_id=self.gpu_id, det_thresh=self.det_thresh)

    def embedding(self, content):
        embeddings = []
        input_image = cv2.imdecode(np.frombuffer(content, dtype=np.uint8), 1)
        faces = self.model.get(input_image)
        for face in faces:
            embedding = np.array(face.embedding).reshape((1, -1))
            embedding = preprocessing.normalize(embedding)
            embeddings.append(embedding)
        return embeddings

    def recognition(self, faces, compare_faces):
        face_ids = list()
        for face in faces:
            face_ids.append(-1)
            for com_face in compare_faces:
                if self.feature_compare(face, np.array(com_face["feature"]), self.threshold):
                    face_id = com_face["face_id"]
                    face_ids[-1] = face_id
                    break
        return face_ids

    @staticmethod
    def feature_compare(feature1, feature2, threshold):
        diff = np.subtract(feature1, feature2)
        dist = np.sum(np.square(diff), 1)
        if dist < threshold:
            return True
        else:
            return False
