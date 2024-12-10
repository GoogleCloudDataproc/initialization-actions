import tensorflow as tf
print("Get GPU Details : ")
print(tf.config.list_physical_devices('GPU'))
#print(tf.test.is_gpu_available())

if tf.test.gpu_device_name():
    print('Default GPU Device:{}'.format(tf.test.gpu_device_name()))
    print("Please install GPU version of TF")

gpu_available = tf.config.list_physical_devices('GPU')
print("gpu_available : " + str(gpu_available))

#is_cuda_gpu_available = tf.config.list_physical_devices('GPU',cuda_only=True)
is_cuda_gpu_available = tf.test.is_gpu_available(cuda_only=True)
print("is_cuda_gpu_available : " + str(is_cuda_gpu_available))

#is_cuda_gpu_min_3 = tf.config.list_physical_devices('GPU',True, (3,0))
is_cuda_gpu_min_3 = tf.test.is_gpu_available(True, (3,0))
print("is_cuda_gpu_min_3 : " + str(is_cuda_gpu_min_3))

from tensorflow.python.client import device_lib

def get_available_gpus():
    local_device_protos = device_lib.list_local_devices()
    return [x.name for x in local_device_protos if x.device_type == 'GPU']

print("Run GPU Functions Below : ")
print(get_available_gpus())
