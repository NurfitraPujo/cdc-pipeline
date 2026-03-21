import { createStartClient } from '@tanstack/start/client'
import { createRouter } from './router'

const router = createRouter()

createStartClient(router)
