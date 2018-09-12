pipeline {
  agent any
  stages {
    stage('stage1') {
      steps {
        sh 'echo 123'
      }
    }
    stage('stage2') {
      parallel {
        stage('stage2') {
          steps {
            timeout(time: 10)
          }
        }
        stage('stage2.1') {
          steps {
            echo 'abc'
          }
        }
      }
    }
    stage('') {
      steps {
        writeFile(file: '111', text: '1111')
      }
    }
  }
}