pipeline {
  agent any
  stages {
    stage('stage1') {
      steps {
        sh 'echo 123'
        input(message: 'should continue', id: 'abc', ok: 'Yes')
      }
    }
    stage('stage2.1') {
      steps {
        echo 'abc'
        sleep 5
        input(message: 'continue', id: 'c2', ok: 'Yes')
      }
    }
    stage('writefile') {
      steps {
        writeFile(file: '111', text: '1111')
      }
    }
  }
}